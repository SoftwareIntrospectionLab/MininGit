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
import re
from io import BytesIO

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
    
    def run(self, repo, uri, db):
        def write_line(data, io):
            io.write(data)
            
        # Start the profiler, per every other extension
        profiler_start("Running bug prediction extension")

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
            
        # Get the commits from this repository
        query = """select s.id, s.rev from scmlog s
            where s.repository_id = ?"""
        read_cursor.execute(statement(query, db.place_holder), (repo_id,))

        self.__prepare_table(connection)

        for row in read_cursor:
            io = BytesIO()
            row_id = row[0]
            rev = row[1]
            
            update = """update scmlog
                        set file_count = ?
                        where id = ?"""

            wid = repo.add_watch(LS, write_line, io)
            
            try:
                repo.ls(path, rev)
            except Exception as e:
                printerr("Error obtaining File Count. Exception: %s", \
                        (str(e),))

            repo.remove_watch(LS, wid)

            try:
                ls_listing = io.getvalue()
            except Exception, e:
                printerr("Error getting contents." +
                            "Exception: %s", (str(e),))
                continue

            execute_statement(statement(update, db.place_holder), 
                              (len(ls_listing.splitlines()), row_id), 
                              write_cursor,
                              db,
                              "Couldn't update scmlog",
                              exception=ExtensionRunError)
            io.close()

        read_cursor.close()
        connection.commit()
        connection.close()

        # This turns off the profiler and deletes its timings
        profiler_stop("Running FileCount extension", delete=True)

register_extension("FileCount", FileCount)
