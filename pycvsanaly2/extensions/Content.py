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
from pycvsanaly2.Database import SqliteDatabase, MysqlDatabase, \
        TableAlreadyExists, statement, DBFile
from pycvsanaly2.utils import printdbg, printerr, printout, \
        remove_directory, uri_to_filename
from pycvsanaly2.profile import profiler_start, profiler_stop
from FileRevs import FileRevs
from repositoryhandler.backends import RepositoryCommandError
from tempfile import mkdtemp, NamedTemporaryFile
from repositoryhandler.backends.watchers import CAT
from Jobs import JobPool, Job
import os
import re

# This class holds a single repository retrieve task,
# and keeps the source code until the object is garbage-collected
class ContentJob(Job):
    def __init__(self, repo, commit_id, file_id, repo_uri, rev, path):
        self.repo = repo
        self.commit_id = commit_id
        self.file_id = file_id
        self.repo_uri = repo_uri
        self.rev = rev
        self.path = path
        self.file_contents = ""

    def write_file(self, line, fd):
        fd.write(line)
    
    def run(self, repo, repo_uri):
        self.repo_type = self.repo.get_type()

        if self.repo_type == 'cvs':
            # CVS self.paths contain the module stuff
            uri = self.repo.get_uri_for_self.path(self.repo_uri)
            module = uri[len(self.repo.get_uri()):].strip('/')

            if module != '.':
                self.path = self.path[len(module):].strip('/')
            else:
                self.path = self.path.strip('/')
        else:
            self.path = self.path.strip('/')

        suffix = ''
        filename = os.path.basename(self.path)
        ext_ptr = filename.rfind('.')
        if ext_ptr != -1:
            suffix = filename[ext_ptr:]

        # Write out to a temporary file
        fd = NamedTemporaryFile('w', suffix=suffix)

        # Not sure what this does yet
        wid = self.repo.add_watch(CAT, self.write_file, fd.file)
        
        # Git doesn't need retries because all of the revisions
        # are already on disk
        if self.repo_type == 'git':
            retries = 0
        else:
            retries = 3
            
        done = False
        failed = False

        # Try downloading the file revision
        while not done and not failed:
            try:
                self.repo.cat(os.path.join(self.repo_uri, self.path), self.rev)
                done = True
            except RepositoryCommandError, e:
                if retries > 0:
                    printerr("Command %s returned %d(%s), try again",\
                            (e.cmd, e.returncode, e.error))
                    retries -= 1
                    fd.file.seek(0)
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
                
        self.repo.remove_watch(CAT, wid)

        if failed:
            #self.measures.set_error()
            printerr("Failure due to error")
        else:
            try:
                f = open(fd.name)
                #print "Dump: " + str(f.readlines())
                for line in f:
                    self.file_contents = self.file_contents + line

                f.close()
                #fm = create_file_metrics(fd.name)
                #self.__measure_file(fm, self.measures, fd.name, self.rev)
            except Exception, e:
                printerr("Error getting contents for for %s@%s. " +
                            "Exception: %s",(fd.name, self.rev, str(e)))
            finally:
                fd.file.close()
                fd.close()

        # Returning a value is probably *not* what run does, but we'll just
        # assume it for now.
        return self.file_contents

    def get_commit_id(self):
        return self.commit_id

    def get_file_contents(self):
        # An encode will fail if the source code can't be converted to
        # utf-8, ie. it's not already unicode, or latin-1, or something
        # obvious. This almost always means that the file isn't source
        # code at all. 
        # TODO: I should really throw a "not source" exception,
        # but just doing None is fine for now.
        try:
            return self.file_contents.encode("utf-8")
        except UnicodeDecodeError, e:
            return None

    def get_file_id(self):
        return self.file_id


class Content(Extension):
    deps = ['FileTypes']
    
    def __prepare_table(self, connection, drop_table=True):
        cursor = connection.cursor()

        # Drop the table's old data
        if drop_table:
            try:
                cursor.execute("DROP TABLE content")
            except:
                # Shouldn't pass on this really, but it will complain if
                # content isn't there, and I can't bothered to worry
                # about it right now
                pass

        if isinstance(self.db, SqliteDatabase):
            import pysqlite2.dbapi2
            
            try:
                cursor.execute("CREATE TABLE content(" +
                    "id INTEGER PRIMARY KEY," +
                    "scmlog_id INTEGER NOT NULL," +
                    "file_id INTEGER NOT NULL," +
                    "content CLOB NOT NULL)")
            except pysqlite2.dbapi2.OperationalError:
                cursor.close()
                raise TableAlreadyExists
            except:
                raise
        elif isinstance(self.db, MysqlDatabase):
            import _mysql_exceptions

            try:
                cursor.execute("CREATE TABLE content(" +
                    "id int(11) NOT NULL auto_increment," +
                    "scmlog_id int(11) NOT NULL," +
                    "file_id int(11) NOT NULL," +
                    "content mediumtext NOT NULL,"
                    "PRIMARY KEY(id)" +
                    ") ENGINE=InnoDB CHARACTER SET=utf8")
            except _mysql_exceptions.OperationalError, e:
                if e.args[0] == 1050:
                    cursor.close()
                    raise TableAlreadyExists
                raise
            except:
                raise
            
        connection.commit()
        cursor.close()

    def __process_finished_jobs(self, job_pool, write_cursor):
        finished_job = job_pool.get_next_done()

        # scmlog_id is the commit ID. For some reason, the 
        # documentaion advocates tablename_id as the reference,
        # but in the source, these are referred to as commit IDs.
        # Don't ask me why!
        while finished_job is not None:
            if finished_job.get_file_contents() is not None:
                write_cursor.execute( \
                        "insert into content(scmlog_id, file_id, content) " +
                        "values(?,?,?)", (finished_job.get_commit_id(), \
                                finished_job.get_file_id(), \
                                str(finished_job.get_file_contents())))
            finished_job = job_pool.get_next_done(0.5)
            


    def run(self, repo, uri, db):
        # Start the profiler, per every other extension
        profiler_start("Running content extension")

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
                    db.place_holder),(repo_uri,))
            repo_id = read_cursor.fetchone()[0]
        except NotImplementedError:
            raise ExtensionRunError( \
                    "Content extension is not supported for %s repos" \
                    %(repo.get_type()))
        except Exception, e:
            raise ExtensionRunError( \
                    "Error creating repository %s. Exception: %s" \
                    %(repo.get_uri(), str(e)))
            
        # Try to create a table for storing the content
        # TODO: Removed use case for choosing between all or just the HEAD,
        # should ideally put that back again. Just all for now is fine.
        try:
            self.__prepare_table(connection)
        except Exception, e:
            raise ExtensionRunError(str(e))

        queuesize = 10

        # This is where the threading stuff comes in, I expect
        # Commenting out as I don't really want to mess with this right now
        job_pool = JobPool(repo, path or repo.get_uri(), queuesize=queuesize)

        # This filters files if they're not source files.
        # I'm pretty sure "unknown" is returning binary files too, but
        # these are implicitly left out when trying to convert to utf-8
        # after download
        query = "select f.id from file_types ft, files f " + \
                "where f.id = ft.file_id and " + \
                "ft.type in('code', 'unknown') and " + \
                "f.repository_id = ?"
        read_cursor.execute(statement(query, db.place_holder),(repo_id,))
        code_files = [item[0] for item in read_cursor.fetchall()]

        fr = FileRevs(db, connection, read_cursor, repo_id)

        i = 0

        # Loop through each file and its revision
        for revision, commit_id, file_id, action_type, composed in fr:
            if file_id not in code_files:
                continue

            try:
                relative_path = fr.get_path()
            except AttributeError, e:
                raise e

            if composed:
                rev = revision.split("|")[0]
            else:
                rev = revision

            printdbg("Path for %d at %s -> %s",(file_id, rev, relative_path))

            # Ignore SVN tags
            if repo.get_type() == 'svn' and relative_path == 'tags':
                printdbg("Skipping file %s",(relative_path,))
                continue

            # Threading stuff commented out
            job = ContentJob(repo, commit_id, file_id, uri, rev, relative_path)
            job_pool.push(job)

            if i >= queuesize:
                printdbg("Queue is now at %d, flushing to database", (i,))
                job_pool.join()
                self.__process_finished_jobs(job_pool, write_cursor)
                connection.commit()
                i = 0
            else:
                i = i + 1

        #job_pool.join()
        #self.__process_finished_jobs(job_pool, write_cursor, True)
                
        profiler_start("Inserting results in db")
        #self.__insert_many(write_cursor)
        connection.commit()
        profiler_stop("Inserting results in db")

        read_cursor.close()
        write_cursor.close()
        connection.close()

        # This turns off the profiler and deletes it's timings
        profiler_stop("Running content extension", delete=True)

register_extension("Content", Content)
