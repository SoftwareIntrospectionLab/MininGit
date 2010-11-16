'''
Created on Nov 4, 2010

@author: linzhp
'''
from Blame import BlameJob, Blame
from pycvsanaly2.extensions import register_extension, ExtensionRunError
from pycvsanaly2.profile import profiler_start, profiler_stop
from pycvsanaly2.utils import printdbg, printerr, uri_to_filename
from pycvsanaly2.Database import (SqliteDatabase, MysqlDatabase, TableAlreadyExists,
                                  statement)
from Jobs import JobPool
from FilePaths import FilePaths


class HunkBlameJob(BlameJob):
    class BlameContentHandler(BlameJob.BlameContentHandler):
        def __init__(self, start_line, end_line):
            self.start_line = start_line
            self.end_line = end_line
            self.bug_revs = set()

        def line(self,blame_line):
            if(blame_line.line>=self.start_line and blame_line.line<=self.end_line):
                self.bug_revs.add(blame_line.rev)

        def start_file (self, filename):
            pass
        def end_file (self):
            pass

    def __init__ (self, hunk_id, path, rev, start_line, end_line):
        self.hunk_id = hunk_id
        self.path = path
        self.rev = rev
        self.start_line = start_line
        self.end_line = end_line
        self.bug_revs = set()
        

    def get_content_handler(self):
        return self.BlameContentHandler(self.start_line, self.end_line)
    
    def collect_results(self, content_handler):
        self.bug_revs = content_handler.bug_revs
        #assert len(self.bug_revs)<2
        
    def get_bug_revs(self):
        return self.bug_revs
    
    def get_hunk_id(self):
        return self.hunk_id
            
class HunkBlame(Blame):
    '''
    classdocs
    '''
    deps = ['Hunks']

    MAX_BLAMES = 1

    # Insert query
    __insert__ = 'INSERT INTO hunk_blames (hunk_id, bug_rev) ' + \
                 'VALUES (?,?)'
    def __init__(self):
        self.id_counter = 1 #Only to conform the interface of superclass
        
    def __create_table(self, cnn):
        cursor = cnn.cursor ()

        if isinstance (self.db, SqliteDatabase):
            import sqlite3.dbapi2
            try:
                cursor.execute ("CREATE TABLE hunk_blames (" +
                                "id integer primary key," +
                                "hunk_id integer," +
                                "bug_rev string"
                                ")")
            except sqlite3.dbapi2.OperationalError:
                cursor.close ()
                raise TableAlreadyExists
            except:
                raise
        elif isinstance (self.db, MysqlDatabase):
            import _mysql_exceptions

            try:
                cursor.execute ("CREATE TABLE hunk_blames (" +
                                "id integer primary key auto_increment," +
                                "hunk_id integer REFERENCES hunks(id)," +
                                "bug_rev mediumtext REFERENCES scmlog(rev)"+
                                ") CHARACTER SET=utf8")
            except _mysql_exceptions.OperationalError, e:
                if e.args[0] == 1050:
                    cursor.close ()
                    raise TableAlreadyExists
                raise
            except:
                raise

        cnn.commit ()
        cursor.close ()

    def __get_hunk_blames(self, cursor, repoid):
        query = """select distinct b.hunk_id 
            from hunk_blames b 
            join hunks h on b.hunk_id=h.id
            join files f on h.file_id=f.id
            where f.repository_id=?"""
        cursor.execute (statement (query, self.db.place_holder), (repoid,))
        return [h[0] for h in cursor.fetchall()]

    def populate_insert_args(self, job):
        bug_revs = job.get_bug_revs ()
        hunk_id = job.get_hunk_id ()
        return [(hunk_id, rev) for rev in bug_revs]
        
    def run (self, repo, uri, db):
        profiler_start ("Running HunkBlame extension")
        
        self.db = db

        cnn = self.db.connect ()
        read_cursor = cnn.cursor ()
        write_cursor = cnn.cursor ()
        try:
            path = uri_to_filename (uri)
            if path is not None:
                repo_uri = repo.get_uri_for_path (path)
            else:
                repo_uri = uri

            read_cursor.execute (statement ("SELECT id from repositories where uri = ?", db.place_holder), (repo_uri,))
            repoid = read_cursor.fetchone ()[0]
        except NotImplementedError:
            raise ExtensionRunError ("HunkBlame extension is not supported for %s repositories" % (repo.get_type ()))
        except Exception, e:
            raise ExtensionRunError ("Error creating repository %s. Exception: %s" % (repo.get_uri (), str (e)))

        try:
            self.__create_table (cnn)
        except TableAlreadyExists:
            pass
        except Exception, e:
            raise ExtensionRunError (str(e))
        
        blames = self.__get_hunk_blames (read_cursor, repoid)

        job_pool = JobPool (repo, path or repo.get_uri (), queuesize=100)

        query = """select h.id, h.file_id, h.commit_id, h.new_start_line, h.new_end_line, s.rev 
                    from hunks h, scmlog s
                    where h.commit_id=s.id and s.repository_id=?"""
        read_cursor.execute(statement (query, db.place_holder), (repoid,))
        hunk = read_cursor.fetchone()
        n_blames = 0
        fp = FilePaths(db)
        fp.update_all(repoid)
        
        while hunk is not None:
            hunk_id, file_id, commit_id, start_line, end_line, rev = hunk
            
            if hunk_id in blames:
                printdbg ("Blame for hunk %d is already in the database, skip it", (hunk_id,))
            else:
                relative_path = fp.get_path(file_id, commit_id, repoid)
                printdbg ("Path for %d at %s -> %s", (file_id, rev, relative_path))

                if relative_path is not None:
                    job = HunkBlameJob (hunk_id, relative_path, rev, start_line, end_line)
                    job_pool.push (job)
                    n_blames += 1
                else:
                    printerr("Couldn't find path for file ID %d", (file_id,))
                    break
                    continue
                
                if n_blames >= self.MAX_BLAMES:
                    self._process_finished_jobs (job_pool, write_cursor)
                    n_blames = 0
            
            hunk = read_cursor.fetchone()

        job_pool.join ()
        self._process_finished_jobs (job_pool, write_cursor, True)

        read_cursor.close ()
        write_cursor.close ()
        cnn.close()

        profiler_stop ("Running HunkBlame extension", delete = True)

register_extension ("HunkBlame", HunkBlame)