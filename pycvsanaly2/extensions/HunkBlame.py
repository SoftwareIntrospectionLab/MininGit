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

from Blame import BlameJob, Blame
from pycvsanaly2.extensions import register_extension, ExtensionRunError
from pycvsanaly2.profile import profiler_start, profiler_stop
from pycvsanaly2.utils import printdbg, printerr, uri_to_filename
from pycvsanaly2.Database import (SqliteDatabase, MysqlDatabase, TableAlreadyExists,
                                  statement)
from Jobs import JobPool, Job
from FilePaths import FilePaths


class HunkBlameJob(BlameJob):
    class BlameContentHandler(BlameJob.BlameContentHandler):
        def __init__(self, hunks):
            self.hunks = hunks
            self.bug_revs = {}

        def line(self,blame_line):
            for hunk_id, start_line, end_line in self.hunks:
                if blame_line.line>= start_line and blame_line.line<= end_line:
                    printdbg("Found a bug revision: %s"%blame_line.rev)
                    if self.bug_revs.get(hunk_id) is None:
                        self.bug_revs[hunk_id] = set()
                    self.bug_revs[hunk_id].add(blame_line.rev)
                    break

        def start_file (self, filename):
            pass
        def end_file (self):
            pass

    def __init__ (self, hunks, path, rev):
        Job.__init__(self)
        self.hunks = hunks
        self.path = path
        self.rev = rev
        self.bug_revs = {}

    def get_content_handler(self):
        return self.BlameContentHandler(self.hunks)
    
    def collect_results(self, content_handler):
        self.bug_revs = content_handler.bug_revs
        
    def get_bug_revs(self):
        return self.bug_revs
        
class NotValidHunkWarning(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)

class HunkBlame(Blame):

#    deps = ['Hunks']

    MAX_BLAMES = 20

    # Insert query
    __insert__ = 'INSERT INTO hunk_blames (hunk_id, bug_commit_id) ' + \
                 'VALUES (?,?)'
    def __init__(self):
        #Only to conform the interface of superclass
        self.id_counter = 1 
        
    def __create_table(self, cnn):
        cursor = cnn.cursor ()

        if isinstance (self.db, SqliteDatabase):
            import sqlite3.dbapi2
            try:
                cursor.execute ("CREATE TABLE hunk_blames (" +
                                "id integer primary key," +
                                "hunk_id integer," +
                                "bug_commit_id integer"
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
                                "bug_commit_id integer REFERENCES scmlog(id)"+
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
    
    # It is also possible to get previous commit by modifying
    # PatchParser.iter_file_patch
    def __find_previous_commit(self, file_id, commit_id):
        query = """select a.commit_id, a.action_type, c.rev from action_files a,scmlog c
            where a.commit_id=c.id and a.file_id=?
            order by c.date
        """
        cnn = self.db.connect ()
        aux_cursor = cnn.cursor()
        aux_cursor.execute(statement(query, self.db.place_holder),(file_id))
        all_commits=aux_cursor.fetchall()
        aux_cursor.close()
        cnn.close()
        pre_commit_id = None
        pre_rev = None
        for cur_commit_id,type, cur_rev in all_commits:
            if cur_commit_id == commit_id:
                #Nothing to blame for other types
                if type != 'M' and type != 'R':
                    raise NotValidHunkWarning("Wrong commit to blame: commit type: %s"%type)
                else:
                    break
            else:
                pre_commit_id = cur_commit_id
                pre_rev = cur_rev
        else:
            raise NotValidHunkWarning("No previous commit found for file %d at commit %d"%(file_id, commit_id))
        if pre_commit_id is None or pre_rev is None:
            raise NotValidHunkWarning("No previous commit found for file %d at commit %d"%(file_id, commit_id))
        return pre_commit_id,pre_rev    

    def populate_insert_args(self, job):
        bug_revs = job.get_bug_revs ()
        cnn = self.db.connect()
        cursor = cnn.cursor()
        args = []
        for hunk_id in bug_revs:
            for rev in bug_revs[hunk_id]:
                printdbg("Find id for rev %s"%rev)
                query = "select id from scmlog where rev = ?"
                cursor.execute(statement(query, self.db.place_holder),(rev,))
                
                fetched_row = cursor.fetchone()
                
                if fetched_row is not None:
                    args.append((hunk_id,fetched_row[0]))
                    
        cursor.close()
        cnn.close()
        return args
        
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
        
        outer_query = """select distinct h.file_id, h.commit_id
            from hunks h, scmlog s
            where h.commit_id=s.id and s.repository_id=?
                and s.is_bug_fix=1
                and h.old_start_line is not null 
                and h.old_end_line is not null
                and h.file_id is not null
                and h.commit_id is not null
        """
        read_cursor.execute(statement (outer_query, db.place_holder), (repoid,))
        file_rev = read_cursor.fetchone()
        n_blames = 0
        fp = FilePaths(db)
        while file_rev is not None:
            try:
                file_id, commit_id = file_rev
                pre_commit_id, pre_rev = self.__find_previous_commit(file_id, commit_id)
                relative_path = fp.get_path(file_id, pre_commit_id, repoid)
                if relative_path is None:
                    raise NotValidHunkWarning("Couldn't find path for file ID %d"%file_id)
                printdbg ("Path for %d at %s -> %s", (file_id, pre_rev, relative_path))
                with cnn as inner_cursor:
                    inner_query = """select h.id, h.old_start_line, h.old_end_line from hunks h
                        where h.file_id = ? and h.commit_id = ?
                    """
                    inner_cursor.execute(statement(inner_query, db.place_holder), (file_id, commit_id))
                    hunks = inner_cursor.fetchall()
                hunks = [h for h in hunks if h[0] not in blames]
                job = HunkBlameJob(hunks, relative_path, pre_rev)
                
                job_pool.push (job)
                n_blames += 1
        
                if n_blames >= self.MAX_BLAMES:
                    self.process_finished_jobs (job_pool, write_cursor)
                    n_blames = 0
            except NotValidHunkWarning as e:
                printerr("Not a valid hunk: "+str(e))
            finally:
                file_rev = read_cursor.fetchone()

        job_pool.join ()
        self.process_finished_jobs (job_pool, write_cursor, True)

        read_cursor.close ()
        write_cursor.close ()
        cnn.close()

        profiler_stop ("Running HunkBlame extension", delete = True)

register_extension ("HunkBlame", HunkBlame)