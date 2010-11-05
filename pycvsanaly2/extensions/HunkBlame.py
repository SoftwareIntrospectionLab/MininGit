'''
Created on Nov 4, 2010

@author: linzhp
'''
from Blame import BlameJob
from pycvsanaly2.extensions import Extension, register_extension, ExtensionRunError
from pycvsanaly2.profile import profiler_start, profiler_stop
from pycvsanaly2.utils import printdbg, printerr, uri_to_filename
from pycvsanaly2.Database import (SqliteDatabase, MysqlDatabase, TableAlreadyExists,
                                  statement)
from Jobs import JobPool
from FilePaths import FilePaths


class HunkBlameJob(BlameJob):
    class BlameContentHandler(BlameJob.BlameContentHandler):
        def line(self,line):
            print line
        def start_file (self, filename):
            pass
        def end_file (self):
            pass

            
class HunkBlame(Extension):
    '''
    classdocs
    '''

    MAX_BLAMES = 1

    def __init__(self):
        '''
        Constructor
        '''
    def __process_finished_jobs (self, job_pool, write_cursor, unlocked = False):
        pass
    
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

        job_pool = JobPool (repo, path or repo.get_uri (), queuesize=100)

        query = "select h.id, h.file_id, h.commit_id, h.start_line, h.end_line, s.rev from hunks h join scmlog s on h.commit_id=s.id " + \
            "where s.repository_id=?"
        read_cursor.execute(statement (query, db.place_holder), (repoid,))
        hunk =read_cursor.fetchone()
        n_blames = 0
        fp = FilePaths(db)
        aux_cursor = cnn.cursor()
        while hunk!=None:
            hunk_id, file_id, commit_id, start_line, end_line, rev = hunk
            relative_path = fp.get_path(file_id, commit_id, repoid)
            printdbg ("Path for %d at %s -> %s", (file_id, rev, relative_path))
            job = BlameJob (file_id, commit_id, relative_path, rev)
            job_pool.push (job)
            n_blames += 1

            if n_blames >= self.MAX_BLAMES:
                self.__process_finished_jobs (job_pool, write_cursor)
                n_blames = 0
            hunk=read_cursor.fetchone()
        aux_cursor.close();

        job_pool.join ()
        self.__process_finished_jobs (job_pool, write_cursor, True)

        read_cursor.close ()
        write_cursor.close ()
        cnn.close()

        profiler_stop ("Running HunkBlame extension", delete = True)

register_extension ("HunkBlame", HunkBlame)