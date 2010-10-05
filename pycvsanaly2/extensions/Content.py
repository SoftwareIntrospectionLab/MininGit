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

class Content(Extension):
    deps = ['FileTypes']
    
    def __create_table (self, connection, drop_table=True):
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

        if isinstance (self.db, SqliteDatabase):
            import pysqlite2.dbapi2
            
            try:
                cursor.execute("CREATE TABLE content (" +
                                "id integer)")
            except pysqlite2.dbapi2.OperationalError:
                cursor.close ()
                raise TableAlreadyExists
            except:
                raise
        elif isinstance (self.db, MysqlDatabase):
            import _mysql_exceptions

            try:
                cursor.execute ("CREATE TABLE content (" +
                                "id integer" +
                                ") CHARACTER SET=utf8")
            except _mysql_exceptions.OperationalError, e:
                if e.args[0] == 1050:
                    cursor.close ()
                    raise TableAlreadyExists
                raise
            except:
                raise
            
        connection.commit ()
        cursor.close ()


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
