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
import os
import re

class Content(Extension):
    deps = ['FileTypes']

    def __job_run(self, repo, repo_uri, rev, path):
        def write_file(line, fd):
            fd.write(line)
            
        repo_type = repo.get_type()
        if repo_type == 'cvs':
            # CVS paths contain the module stuff
            uri = repo.get_uri_for_path(repo_uri)
            module = uri[len(repo.get_uri()):].strip('/')

            if module != '.':
                path = path[len(module):].strip('/')
            else:
                path = path.strip('/')
        else:
            path = path.strip('/')

        suffix = ''
        filename = os.path.basename(path)
        ext_ptr = filename.rfind('.')
        if ext_ptr != -1:
            suffix = filename[ext_ptr:]

        fd = NamedTemporaryFile('w', suffix=suffix)
        wid = repo.add_watch(CAT, write_file, fd.file)
            
        if repo_type == 'git':
            retries = 0
        else:
            retries = 3
            
        done = False
        failed = False
        while not done and not failed:
            try:
                repo.cat(os.path.join(repo_uri, path), rev)
                done = True
            except RepositoryCommandError, e:
                if retries > 0:
                    printerr("Command %s returned %d(%s), try again",(e.cmd, e.returncode, e.error))
                    retries -= 1
                    fd.file.seek(0)
                elif retries == 0:
                    failed = True
                    printerr("Error obtaining %s@%s. Command %s returned %d(%s)",
                             (path, rev, e.cmd, e.returncode, e.error))
            except Exception, e:
                failed = True
                printerr("Error obtaining %s@%s. Exception: %s",(path, self.rev, str(e)))
                
        repo.remove_watch(CAT, wid)
        fd.file.close()

        if failed:
            self.measures.set_error()
        else:
            try:
                f = open(fd.name)
                #print "Dump: " + str(f.readlines())
                return str(f.readlines())
                #fm = create_file_metrics(fd.name)
                #self.__measure_file(fm, self.measures, fd.name, self.rev)
            except Exception, e:
                printerr("Error creating FileMetrics for %s@%s. Exception: %s",(fd.name, self.rev, str(e)))

        fd.close()
    
    def __create_table(self, connection, drop_table=True):
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
                                "id int(11) primary key," +
                                "repository_id int(11) NOT NULL," +
                                "content clob NOT NULL)")
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
                                "repository_id int(11) NOT NULL default '0'," +
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


    def run(self, repo, uri, db):
        # Start the profiler, per every other extension
        profiler_start("Running content extension")

        # Open a connection to the database and get cursors
        self.db = db
        connection = self.db.connect()
        read_cursor = connection.cursor()
        write_cursor = connection.cursor()
        
        id_counter = 1

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
            self.__create_table(connection)
        except Exception, e:
            raise ExtensionRunError(str(e))

        # This is where the threading stuff comes in, I expect
        # Commenting out as I don't really want to mess with this right now
        #job_pool = JobPool(repo, path or repo.get_uri(), queuesize=self.MAX_METRICS)

        # Get code files to discard all other files in case of metrics-all
        # -- This filters files if they're not source files, I'm not sure
        # why you would ever not want this on, metrics-all or not.
        query = "select f.id from file_types ft, files f " + \
                "where f.id = ft.file_id and " + \
                "ft.type in('code', 'unknown') and " + \
                "f.repository_id = ?"
        read_cursor.execute(statement(query, db.place_holder),(repo_id,))
        code_files = [item[0] for item in read_cursor.fetchall()]

        fr = FileRevs(db, connection, read_cursor, repo_id)

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

            if repo.get_type() == 'svn' and relative_path == 'tags':
                printdbg("Skipping file %s",(relative_path,))
                continue

            # Threading stuff commented out
            #job = MetricsJob(id_counter, file_id, commit_id, relative_path, rev, failed)
            #job_pool.push(job)
            file_content = self.__job_run(repo, uri, rev, relative_path)
            print "Got file content of: " + str(file_content)

            write_cursor.execute("insert into content(repository_id, content) values(?,?)", (repo_id, str(file_content)))
            connection.commit()
            

        #job_pool.join()
        #self.__process_finished_jobs(job_pool, write_cursor, True)
                
        profiler_start("Inserting results in db")
        #self.__insert_many(write_cursor)
        connection.commit()
        profiler_stop("Inserting results in db")

        read_cursor.close()
        write_cursor.close()
        connection.close()

        printout("Hello extension world!")

        # This turns off the profiler and deletes it's timings
        profiler_stop("Running content extension", delete=True)

register_extension("Content", Content)
