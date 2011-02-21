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
    
    def __match_string(self, regexes, flags, string):
        """Checks whether a string matches a series of regexes"""
        for r in regexes:
            # The bit at the beginning and end matches whitespace, punctuation
            # or the start or end of a line.
            delimiters = "[\s\.,;\!\?\'\"\/\\\]"
            if re.search("(" + delimiters + "+|^)" + r + "(" + delimiters + "+|$)", string, flags):
                printdbg("[STRING] matched on " + str(r) + " " + string)
                return True
                
        return False


    def fixes_bug(self, commit_message):
        """Check whether a commit message indicated a bug was present.
        
        # This is set in the config. Uncomment if you wish to try out
        # specific regexes
        #>>> Config().bug_fix_regexes = ["defect(s)?", "patch(ing|es|ed)?", \
                "bug(s|fix(es)?)?", "debug(ged)?", "fix(es|ed)?", "\#\d+"]
        #>>> Config().bug_fix_regexes_case_sensitive = ["[A-Z]+-\d+",]
        >>> b = BugFixMessage()
        
        # Easy ones
        >>> b.fixes_bug("Bug")
        True
        >>> b.fixes_bug("Bugs")
        True
        >>> b.fixes_bug("Fix")
        True
        >>> b.fixes_bug("Fixed")
        True
        >>> b.fixes_bug("Defect")
        True
        >>> b.fixes_bug("Defects")
        True
        >>> b.fixes_bug("Patches")
        True
        >>> b.fixes_bug("Patching")
        True
        
        # Embeds in sentences
        >>> b.fixes_bug("Fixed a bug")
        True
        >>> b.fixes_bug("Debugged this one")
        True
        >>> b.fixes_bug("Found a hole, which I patched, shouldn't be a problem")
        True
        >>> b.fixes_bug("Put in a couple of fixes in x.java")
        True
        >>> b.fixes_bug("Implemented a bugfix")
        True
        >>> b.fixes_bug("References #1234")
        True
        >>> b.fixes_bug("Defect X is no more")
        True
        >>> b.fixes_bug("Closes JENKINS-1234")
        True
        
        # Embeds in long commit messages
        >>> b.fixes_bug("This was tough. Fixed now.")
        True
        >>> b.fixes_bug("Found X; debugged and solved.")
        True
        
        # Regression tests from Apache
        # When adding these, keep weird punctuation intact.
        >>> b.fixes_bug("Fixups to build the whole shebang once again.")
        True
        >>> b.fixes_bug("Change some INFO messages to DEBUG messages.")
        True
        >>> b.fixes_bug("Put back PR#6347")
        True
        >>> b.fixes_bug("Typo fixage..")
        True
        >>> b.fixes_bug("another typo/fixup")
        True
        >>> b.fixes_bug("Refix the entity tag comparisons")
        True
        >>> b.fixes_bug("Closeout PR#721")
        True
        >>> b.fixes_bug("SECURITY: CVE-2010-0408 (cve.mitre.org)")
        True
        >>> b.fixes_bug("    debugged the require_one and require_all")
        True
        >>> b.fixes_bug("    various style fixups / general changes")
        True
        >>> b.fixes_bug("    Win32: Eliminate useless debug error message")
        True
        
        # Things that shouldn't match
        # Refactoring could go either way, depending on whether you think
        # renaming/refactoring is a "bug fix." Right now, we don't call that
        # a "bug"
        >>> b.fixes_bug("Added method print_debug()")
        False
        >>> b.fixes_bug("Altered debug_log()")
        False
        >>> b.fixes_bug("NETWORK_PATCH_FIX")
        False
        >>> b.fixes_bug("Rename ap_debug_assert() to AP_DEBUG_ASSERT()")
        False
        >>> b.fixes_bug("Use bread() etc instead of fread() for reading/writing")
        False
        >>> b.fixes_bug("Refactored to look cleaner")
        False
        >>> b.fixes_bug("Rewrite this yucky file")
        False
        >>> b.fixes_bug("Edited this file on 2010-12-01")
        False
        >>> b.fixes_bug("This file pertains to the A80-154 spec")
        False
        >>> b.fixes_bug("This is for March-28")
        False
        """
        if self.__match_string(Config().bug_fix_regexes, \
        re.DOTALL | re.IGNORECASE, commit_message):
            return True
        
        if self.__match_string(Config().bug_fix_regexes_case_sensitive, \
        re.DOTALL, commit_message):
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
            
            update = """update scmlog
                        set is_bug_fix = ?
                        where id = ?"""

            if self.fixes_bug(commit_message):
                is_bug_fix = 1
            else:
                is_bug_fix = 0

            execute_statement(statement(update, db.place_holder), 
                              (is_bug_fix, row_id), 
                              write_cursor,
                              db,
                              "Couldn't update scmlog",
                              exception=ExtensionRunError)

        read_cursor.close()
        connection.commit()
        connection.close()

        # This turns off the profiler and deletes its timings
        profiler_stop("Running BugFixMessage extension", delete=True)

register_extension("BugFixMessage", BugFixMessage)
