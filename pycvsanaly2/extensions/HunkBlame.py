# Copyright (C) 2010 University of California, Santa Cruz
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors :
#       Zhongpeng Lin  <zlin5@ucsc.edu>


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
        def __init__(self, job):
            self.db = job.db
            self.cnn = job.cnn
            self.cursor = job.cnn.cursor()
            self.start_line = job.start_line
            self.end_line = job.end_line
            self.bug_hunk_ids = set()
            
            self.rev_hunks_cache = {}
            self.hunk_content_cache = {}

        def line(self,blame_line):
            if blame_line.line>=self.start_line and blame_line.line<=self.end_line:
                rev = blame_line.rev
                cursor = self.cursor
                hunks = self.rev_hunks_cache.get(rev)
                if hunks is None:
                    sql = """select h.id, h.file_id, h.commit_id, h.new_start_line, h.new_end_line 
                        from hunks h, scmlog s 
                        where h.commit_id=s.id and s.rev=?
                    """
                    cursor.execute(statement(sql, self.db.place_holder), (rev,))
                    hunks = cursor.fetchall()
                    self.rev_hunks_cache[rev] = hunks
                for h in hunks:
                    (hunk_id, file_id, commit_id, new_start_line, new_end_line) = h
                    if (file_id and commit_id and new_start_line and new_end_line) is None:
                        continue
                    
                    content_lines = self.hunk_content_cache.get(hunk_id)
                    if content_lines is None:
                        sql = "select content from content where file_id=? and scmlog_id=?"
                        cursor.execute(statement(sql, self.db.place_holder), (file_id, commit_id))
                        result = cursor.fetchone()
                        if result is None:
                            printerr("No content for file_id=%d, scmlog_id=%d",(file_id,commit_id))
                            #Save the null result to avoid touching database again
                            content_lines = ()
                        else:
                            content_str = result[0]
                            content_lines = content_str.splitlines()[new_start_line-1:new_end_line]
                        self.hunk_content_cache[hunk_id] = content_lines
                    for line in content_lines:
                        if blame_line.content == line.strip():
                            print("find bug introducing hunk!")
                            self.bug_hunk_ids.add(hunk_id)
                            return
                else:
                    printerr("No bug introducing hunk found")

        def start_file (self, filename):
            pass
        def end_file (self):
            pass

    def __init__ (self, hunk_id, path, rev, start_line, end_line, cnn, db):
        self.hunk_id = hunk_id
        self.path = path
        self.rev = rev
        self.start_line = start_line
        self.end_line = end_line
        self.cnn = cnn
        self.db = db
        self.bug_hunk_ids = set()
        

    def get_content_handler(self):
        return self.BlameContentHandler(self)
    
    def collect_results(self, content_handler):
        self.bug_hunk_ids = content_handler.bug_hunk_ids
        
    def get_bug_hunk_ids(self):
        return self.bug_hunk_ids
    
    def get_hunk_id(self):
        return self.hunk_id
            
class HunkBlame(Blame):
    '''
    classdocs
    '''
 #   deps = ['Hunks']

    MAX_BLAMES = 1

    # Insert query
    __insert__ = 'INSERT INTO hunk_blames (hunk_id, bug_hunk_id) ' + \
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
                                "bug_hunk_id integer"
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
                                "bug_hunk_id integer REFERENCES hunks(id)"+
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
        bug_hunk_ids = job.get_bug_hunk_ids ()
        hunk_id = job.get_hunk_id ()
        return [(hunk_id, bh) for bh in bug_hunk_ids]        
        
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
                    job = HunkBlameJob (hunk_id, relative_path, rev, start_line, end_line, cnn, db)
                    job_pool.push (job)
                    n_blames += 1
                else:
                    printerr("Couldn't find path for file ID %d", (file_id,))
                    hunk = read_cursor.fetchone()
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