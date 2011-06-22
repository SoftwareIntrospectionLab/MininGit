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
from pycvsanaly2.Database import SqliteDatabase, MysqlDatabase, statement, \
    ICursor, execute_statement
from pycvsanaly2.utils import printdbg, printerr, printout, uri_to_filename
from pycvsanaly2.profile import profiler_start, profiler_stop
from pycvsanaly2.PatchParser import parse_patches, RemoveLine, InsertLine, \
        ContextLine, Patch
import re


class CommitData(object):
    def __init__(self, file_name,
                    old_start_line=None, old_end_line=None, \
                    new_start_line=None, new_end_line=None):
        self.file_name = file_name

        if (old_start_line != old_end_line and \
                (old_start_line is None or old_end_line is None)) or \
            (new_start_line != old_end_line and \
                (new_start_line is None or new_end_line is None)):
            raise ValueError("If either start or end is None, both must be")

        self.old_start_line = old_start_line
        self.old_end_line = old_end_line
        self.new_start_line = new_start_line
        self.new_end_line = new_end_line

    def __str__(self):
        s = "File: " + self.file_name + "\n"

        if self.old_start_line is not None:
            s = s + "Old start line = " + str(self.old_start_line) + "\n"
            s = s + "Old end line = " + str(self.old_end_line) + "\n"
        else:
            s = s + "None deleted\n"

        if self.new_start_line is not None:
            s = s + "New start line = " + str(self.new_start_line) + "\n"
            s = s + "New end line = " + str(self.new_end_line) + "\n"
        else:
            s = s + "None added\n"

        return s.strip()


# This class holds a single repository retrieve task,
# and keeps the source code until the object is garbage-collected
class Hunks(Extension):
    deps = ['Patches']
    INTERVAL_SIZE = 100

    def __prepare_table(self, connection, drop_table=False):
        cursor = connection.cursor()

        # Drop the table's old data
        if drop_table:
            try:
                cursor.execute("DROP TABLE hunks")
            except Exception, e:
                printerr("Couldn't drop hunks table because %s", (e,))

        if isinstance(self.db, SqliteDatabase):
            import sqlite3.dbapi2

            # Note that we can't guarentee sqlite is going
            # to provide foreign key support (it was only
            # introduced in 3.6.19), so no constraints are set
            try:
                cursor.execute("""CREATE TABLE hunks(
                    id INTEGER PRIMARY KEY,
                    file_id INTEGER,
                    commit_id INTEGER NOT NULL,
                    old_start_line INTEGER,
                    old_end_line INTEGER,
                    new_start_line INTEGER,
                    new_end_line INTEGER,
                    bug_introducing INTEGER NOT NULL default 0,
                    bug_introducing_hunk INTEGER,
                    UNIQUE (file_id, commit_id, old_start_line, old_end_line,
                            new_start_line, new_end_line))""")
            except sqlite3.dbapi2.OperationalError:
                # It's OK if the table already exists
                pass
                #raise TableAlreadyExists
            except:
                raise
            finally:
                cursor.close()

        elif isinstance(self.db, MysqlDatabase):
            import MySQLdb

            # I commented out foreign key constraints because
            # cvsanaly uses MyISAM, which doesn't enforce them.
            # MySQL was giving errno:150 when trying to create with
            # them anyway
            try:
                cursor.execute("""CREATE TABLE hunks(
                    id int(11) NOT NULL auto_increment,
                    file_id int(11),
                    commit_id int(11) NOT NULL,
                    old_start_line int(11),
                    old_end_line int(11),
                    new_start_line int(11),
                    new_end_line int(11),
                    bug_introducing bool NOT NULL default false,
                    PRIMARY KEY(id),
                    UNIQUE (file_id, commit_id, old_start_line, old_end_line,
                            new_start_line, new_end_line)
                    ) ENGINE=InnoDB CHARACTER SET=utf8""")
            except MySQLdb.OperationalError, e:
                if e.args[0] == 1050:
                    # It's OK if the table already exists
                    pass
                    #raise TableAlreadyExists
                else:
                    raise
            except:
                raise
            finally:
                cursor.close()

        connection.commit()
        cursor.close()

    def get_commit_data(self, patch_content):
        profiler_start("get_commit_data")
        lines = [l + "\n" for l in patch_content.splitlines() if l]
        hunks = []

        for patch in [p for p in parse_patches(lines, allow_dirty=True, \
                            allow_continue=True) if isinstance(p, Patch)]:
            # This method matches that of parseLine in UnifiedDiffParser.java
            # It's not necessarily intuitive, but this algorithm is much harder
            # than it looks, I spent hours trying to get a simpler solution.
            # It does, however, seem to work, which is pretty amazing when
            # you think about how difficult it is for long enough.
            # The trick that this method does is that each *part* of a hunk
            # ie. added, deleted, changed are treated as *new entities*.
            # The EntityDelta table does not store just diffs, it stores
            # each part of a diff.
            # I will need to copy the behavior of how Sep inserts a CommitData
            # into the database to ensure things match
            for hunk in patch.hunks:
                old_start_line = hunk.orig_pos - 1
                new_start_line = hunk.mod_pos - 1

                old_end_line = 0
                new_end_line = 0

                added = False
                deleted = False
                in_change = False

                for line in hunk.lines:
                    if isinstance(line, RemoveLine):
                        if not in_change or not deleted:
                            in_change = True
                            old_start_line += 1
                            old_end_line = old_start_line
                        else:
                            old_end_line += 1

                        deleted = True

                    elif isinstance(line, InsertLine):
                        if not in_change or not added:
                            in_change = True
                            new_start_line += 1
                            new_end_line = new_start_line
                        else:
                            new_end_line += 1

                        added = True

                    elif isinstance(line, ContextLine):
                        if in_change:
                            in_change = False
                            printdbg("Patch new name: " + patch.newname)
                            file_name = re.split('\s+', patch.newname)[0]
                            if file_name == "/dev/null":
                                file_name = re.split('\s+', patch.oldname)[0]
                            cd = CommitData(file_name)

                            if deleted:
                                cd.old_start_line = old_start_line
                                cd.old_end_line = old_end_line
                                old_start_line = old_end_line

                            if added:
                                cd.new_start_line = new_start_line
                                cd.new_end_line = new_end_line
                                new_start_line = new_end_line

                            hunks.append(cd)
                            added = deleted = False

                        old_start_line += 1
                        new_start_line += 1

                # The diff ended without a new context line
                if in_change:
                    cd = CommitData(re.split('\s+', patch.newname)[0])

                    if deleted:
                        cd.old_start_line = old_start_line
                        cd.old_end_line = old_end_line

                    if added:
                        cd.new_start_line = new_start_line
                        cd.new_end_line = new_end_line

                    hunks.append(cd)
        profiler_stop("get_commit_data")
        return hunks

    def get_patches(self, repo, repo_uri, repo_id, db, cursor):
        profiler_start("Hunks: fetch all patches")
        icursor = ICursor(cursor, self.INTERVAL_SIZE)
        # Get the patches from this repository
        query = """select p.commit_id, p.patch, s.rev
                    from patches p, scmlog s
                    where p.commit_id = s.id and
                    s.repository_id = ? and
                    p.patch is not NULL"""
        icursor.execute(statement(query, db.place_holder), (repo_id,))
        profiler_stop("Hunks: fetch all patches", delete=True)

        rs = icursor.fetchmany()

        while rs:
          for commit_id, patch_content, rev in rs:
            yield (commit_id, patch_content, rev)

          rs = icursor.fetchmany()

    def run(self, repo, uri, db):
        # Start the profiler, per every other extension
        profiler_start("Running hunks extension")

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
                    "Content extension is not supported for %s repos" % \
                    (repo.get_type()))
        except Exception, e:
            raise ExtensionRunError( \
                    "Error creating repository %s. Exception: %s" % \
                    (repo.get_uri(), str(e)))

        self.__prepare_table(connection)
        fp = FilePaths(db)

        patches = self.get_patches(repo, path or repo.get_uri(), repo_id, db,
                                   read_cursor)

        for commit_id, patch_content, rev in patches:
            for hunk in self.get_commit_data(patch_content):
                # Get the file ID from the database for linking
                hunk_file_name = re.sub(r'^[ab]\/', '',
                                        hunk.file_name.strip())
                file_id = fp.get_file_id(hunk_file_name, commit_id)

                if file_id == None:
                    printdbg("file not found")
                    if repo.type == "git":
                        # The liklihood is that this is a merge, not a
                        # missing ID from some data screwup.
                        # We'll just continue and throw this away
                        continue
                    else:
                        printerr("No file ID found for hunk " + \
                                 hunk_file_name + \
                                 " at commit " + str(commit_id))

                insert = """insert into hunks(file_id, commit_id,
                            old_start_line, old_end_line, new_start_line,
                            new_end_line)
                            values(?,?,?,?,?,?)"""

                execute_statement(statement(insert, db.place_holder),
                                  (file_id, commit_id,
                                   hunk.old_start_line,
                                   hunk.old_end_line,
                                   hunk.new_start_line,
                                   hunk.new_end_line),
                                   write_cursor,
                                   db,
                                   "Couldn't insert hunk, dup record?",
                                   exception=ExtensionRunError)

            connection.commit()

        read_cursor.close()
        connection.commit()
        connection.close()

        # This turns off the profiler and deletes its timings
        profiler_stop("Running hunks extension", delete=True)

    def backout(self, repo, uri, db):
        update_statement = """delete from hunks
                              where commit_id in (select s.id from scmlog s
                                          where s.repository_id = ?)"""

        self._do_backout(repo, uri, db, update_statement)

register_extension("Hunks", Hunks)
