# Copyright(C) 2010 University of California, Santa Cruz
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
#(at your option) any later version.
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
#       Chris Lewis <cflewis@soe.ucsc.edu>

from pycvsanaly2.extensions import Extension, register_extension, \
        ExtensionRunError
from pycvsanaly2.extensions.FilePaths import FilePaths
from pycvsanaly2.Database import SqliteDatabase, MysqlDatabase, \
        TableAlreadyExists, statement, execute_statement
from pycvsanaly2.utils import printdbg, printerr, printout, \
        remove_directory, uri_to_filename
from pycvsanaly2.profile import profiler_start, profiler_stop
from pycvsanaly2.Config import Config
from repositoryhandler.backends.watchers import LS
from Jobs import JobPool, Job
from repositoryhandler.backends import RepositoryCommandError
import re
from io import BytesIO
import os


# This class holds a single repository retrieve task,
# and keeps the source code until the object is garbage-collected
class FileCountJob(Job):
    def __init__(self, row_id, rev):
        self.row_id = row_id
        self.rev = rev
        self.ls_lines = ""

    def run(self, repo, repo_uri):
        def write_line(data, io):
            io.write(data)
        
        self.repo = repo
        self.repo_uri = repo_uri
        self.repo_type = self.repo.get_type()

        io = BytesIO()
        wid = repo.add_watch(LS, write_line, io)
        
        # Git doesn't need retries because all of the revisions
        # are already on disk
        if self.repo_type == 'git':
            retries = 0
        else:
            retries = 3
            
        done = False
        failed = False
        
        # Try downloading the file listing
        while not done and not failed:
            try:
                self.repo.ls(self.repo_uri, self.rev)
                done = True
            except RepositoryCommandError, e:
                if retries > 0:
                    printerr("Command %s returned %d(%s), try again",\
                            (e.cmd, e.returncode, e.error))
                    retries -= 1
                    io.seek(0)
                elif retries == 0:
                    failed = True
                    printerr("Error obtaining %s@%s. " +
                                "Command %s returned %d(%s)", \
                                (self.path, self.rev, e.cmd, \
                                e.returncode, e.error))
            except Exception, e:
                failed = True
                printerr("Error obtaining %s@%s. Exception: %s", \
                        (self.path, self.rev, str(e)))

        self.repo.remove_watch(LS, wid)

        if failed:
            printerr("Failure due to error")
        else:
            try:
                self.ls_lines = io.getvalue()
                io.close()
            except Exception, e:
                printerr("Error getting ls-lines." +
                            "Exception: %s", (str(e),))
            finally:
                #TODO: This should close, but it throws an error
                # sometimes. It's fixable using an algorithm like
                # <http://goo.gl/9gPCw>
                #fd.close()
                pass
            
    def _get_ls_line_count(self):
        return len(self.ls_lines.splitlines())
    
    ls_line_count = property(_get_ls_line_count)


class FileCount(Extension):
    def __prepare_table(self, connection):
        cursor = connection.cursor()

        if isinstance(self.db, SqliteDatabase):
            import sqlite3.dbapi2
            
            try:
                cursor.execute("""ALTER TABLE scmlog
                    ADD file_count INTEGER""")
            except sqlite3.dbapi2.OperationalError:
                # It's OK if the column already exists
                pass
            except:
                raise
            finally:
                cursor.close()

        elif isinstance(self.db, MysqlDatabase):
            import _mysql_exceptions

            # I commented out foreign key constraints because
            # cvsanaly uses MyISAM, which doesn't enforce them.
            # MySQL was giving errno:150 when trying to create with
            # them anyway
            try:
                cursor.execute("""ALTER TABLE scmlog
                    ADD file_count int(11)""")
            except _mysql_exceptions.OperationalError, e:
                if e.args[0] == 1060:
                    # It's OK if the column already exists
                    pass
                else:
                    raise
            except:
                raise
            finally:
                cursor.close()
            
        connection.commit()
        cursor.close()
        
    def __process_finished_jobs(self, job_pool, write_cursor, db):
        finished_job = job_pool.get_next_done(0)
        processed_jobs = 0

        while finished_job is not None:
            query = """update scmlog
                        set file_count = ?
                        where id = ?"""
            insert_statement = statement(query, db.place_holder)
            parameters = (finished_job.ls_line_count, finished_job.row_id)
                                
            execute_statement(insert_statement, parameters, write_cursor, db,
                       "Couldn't update scmlog with ls line count", 
                       exception=ExtensionRunError)
            
            processed_jobs += 1
            finished_job = job_pool.get_next_done(0)
            # print "Before return: %s"%(datetime.now()-start)
            
        return processed_jobs
    
    def run(self, repo, uri, db):            
        # Start the profiler, per every other extension
        profiler_start("Running FileCount extension")
        
        # Open a connection to the database and get cursors
        self.db = db
        connection = self.db.connect()
        read_cursor = connection.cursor()
        write_cursor = connection.cursor()
        
        # Try to get the repository and get its ID from the database
        try:
            path = uri_to_filename(uri)
            if path is not None:
                repo_uri = repo.get_uri_for_path(path)
            else:
                repo_uri = uri

            read_cursor.execute(statement( \
                    "SELECT id from repositories where uri = ?", \
                    db.place_holder), (repo_uri,))
            repo_id = read_cursor.fetchone()[0]
        except NotImplementedError:
            raise ExtensionRunError( \
                    "FileCount extension is not supported for %s repos" % \
                    (repo.get_type()))
        except Exception, e:
            raise ExtensionRunError( \
                    "Error creating repository %s. Exception: %s" % \
                    (repo.get_uri(), str(e)))
            
        queuesize = Config().max_threads

        job_pool = JobPool(repo, path or repo.get_uri(), 
                           queuesize=queuesize)
            
        # Get the commits from this repository
        query = """select s.id, s.rev from scmlog s
            where s.repository_id = ?"""
        read_cursor.execute(statement(query, db.place_holder), (repo_id,))

        self.__prepare_table(connection)

        i = 0

        for row in read_cursor:
            row_id = row[0]
            rev = row[1]
            
            job = FileCountJob(row_id, rev)
            job_pool.push(job)
            
            i = i + 1
            
            if i >= queuesize:
                printdbg("FileCount queue is now at %d, flushing to database", 
                         (i,))

                processed_jobs = self.__process_finished_jobs(job_pool, 
                                                              write_cursor, db)

                connection.commit()
                i = i - processed_jobs
                
                if processed_jobs < (queuesize / 5):
                    job_pool.join()
        
        job_pool.join()
        self.__process_finished_jobs(job_pool, write_cursor, db)
        read_cursor.close()
        connection.commit()
        connection.close()

        # This turns off the profiler and deletes its timings
        profiler_stop("Running FileCount extension", delete=True)
        
                
    def backout(self, repo, uri, db):
        update_statement = """update scmlog
                       set file_count = NULL
                       where repository_id = ?"""

        self._do_backout(repo, uri, db, update_statement)

register_extension("FileCount", FileCount)
