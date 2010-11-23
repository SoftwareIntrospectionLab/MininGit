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
        TableAlreadyExists, statement, DBFile
from pycvsanaly2.utils import printdbg, printerr, printout, \
        remove_directory, uri_to_filename
from pycvsanaly2.profile import profiler_start, profiler_stop
from pycvsanaly2.PatchParser import parse_patches, RemoveLine, InsertLine, \
        ContextLine, Patch, BinaryPatch
import os
import re

# This class holds a single repository retrieve task,
# and keeps the source code until the object is garbage-collected
class BugFixMessage(Extension):
    def __prepare_table(self, connection):
        cursor = connection.cursor()

        if isinstance(self.db, SqliteDatabase):
            import sqlite3.dbapi2
            
            try:
                cursor.execute("""ALTER TABLE scmlog
                    ADD is_bug_fix INTEGER default 0""")
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
                    ADD is_bug_fix bool default false""")
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


    # This matches comments about defects, patching, bugs, bugfixes,
    # fixes, references to bug numbers like #1234, and JIRA style
    # comments, like HARMONY-1234 or GH-2.
    def fixes_bug(self, commit_message):
        patterns = ["defect(es)?", "patch(ing)?", "bug(s|fix(es)?)?", 
                "fix(es|ed)?", "\#\d+", "[A-Z]+-\d+"]

        for p in patterns:
            if re.search(p, commit_message, re.DOTALL | re.IGNORECASE):
                return True

        return False


    def run(self, repo, uri, db):
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
                    db.place_holder),(repo_uri,))
            repo_id = read_cursor.fetchone()[0]
        except NotImplementedError:
            raise ExtensionRunError( \
                    "BugPrediction extension is not supported for %s repos" \
                    %(repo.get_type()))
        except Exception, e:
            raise ExtensionRunError( \
                    "Error creating repository %s. Exception: %s" \
                    %(repo.get_uri(), str(e)))
            
        # Get the commit notes from this repository
        query = """select s.id, s.message from scmlog s
            where s.repository_id = ?"""
        read_cursor.execute(statement(query, db.place_holder),(repo_id,))

        self.__prepare_table(connection)
        fp = FilePaths(db)
        fp.update_all(repo_id)

        for row in read_cursor:
            row_id = row[0]
            commit_message = row[1]
            
            if self.fixes_bug(commit_message):
                update = """update scmlog
                            set is_bug_fix= 1
                            where id = ?"""
                try:
                    write_cursor.execute(statement(update, db.place_holder), \
                            (row_id,))
                except Exception, e:
                    printerr("Couldn't update scmlog: " + str(e))
                    continue

        read_cursor.close()
        connection.commit()
        connection.close()

        # This turns off the profiler and deletes its timings
        profiler_stop("Running BugFixMessage extension", delete=True)

register_extension("BugFixMessage", BugFixMessage)
