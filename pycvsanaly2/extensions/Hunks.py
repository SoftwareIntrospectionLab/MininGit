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
from pycvsanaly2.PatchParser import parse_patches, RemoveLine, InsertLine, \
        ContextLine
import os
import re

class CommitData:
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
            s = s +  "Old end line = " + str(self.old_end_line) + "\n"
        else:
            s = s + "None deleted\n"

        if self.new_start_line is not None:
            s = s + "New start line = " + str(self.new_start_line) + "\n"
            s = s +  "New end line = " + str(self.new_end_line) + "\n"
        else:
            s = s + "None added\n"

        return s.strip()

# This class holds a single repository retrieve task,
# and keeps the source code until the object is garbage-collected
class Hunks(Extension):
    deps = ['Patches']

    def get_commit_data(self, patch_content):
        lines = [l + "\n" for l in patch_content.splitlines()]
        commits = []

        for patch in parse_patches(lines, allow_dirty=True):
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
                oldStartLine = hunk.orig_pos - 1
                newStartLine = hunk.mod_pos - 1
                
                oldEndLine = 0
                newEndLine = 0

                added = False
                deleted = False
                in_change = False

                for line in hunk.lines:
                    if isinstance(line, RemoveLine):
                        if not in_change or not deleted:
                            in_change = True
                            oldStartLine += 1
                            oldEndLine = oldStartLine
                        else:
                            oldEndLine += 1
                        
                        deleted = True

                    elif isinstance(line, InsertLine):
                        if not in_change or not added:
                            in_change = True
                            newStartLine += 1
                            newEndLine = newStartLine
                        else:
                            newEndLine += 1

                        added = True

                    elif isinstance(line, ContextLine):
                        if in_change:
                            in_change = False

                            cd = CommitData(re.split('\s+', patch.newname)[0])

                            if deleted:
                                cd.old_start_line = oldStartLine
                                cd.old_end_line = oldEndLine
                                oldStartLine = oldEndLine
                            
                            if added:
                                cd.new_start_line = newStartLine
                                cd.new_end_line = newEndLine
                                newStartLine = newEndLine
                            
                            commits.append(cd)
                            added = deleted = False
                        
                        oldStartLine += 1
                        newStartLine += 1

                # The diff ended without a new context line
                if in_change:
                    cd = CommitData(re.split('\s+', patch.newname)[0])

                    if deleted:
                        cd.old_start_line = oldStartLine
                        cd.old_end_line = oldEndLine
                    
                    if added:
                        cd.new_start_line = newStartLine
                        cd.new_end_line = newEndLine

                    commits.append(cd)

        return commits

    
    def run(self, repo, uri, db):
        # Start the profiler, per every other extension
        profiler_start("Running hunks extension")

        # Open a connection to the database and get cursors
        self.db = db
        connection = self.db.connect()
        read_cursor = connection.cursor()
        read_cursor_1 = connection.cursor()
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
            
        # Get the patches from this repository
        query = "select p.commit_id, p.patch from patches p, scmlog s " + \
                "where p.commit_id = s.id and " + \
                "s.repository_id = ?"
        read_cursor.execute(statement(query, db.place_holder),(repo_id,))

        for row in read_cursor:
            commit_id = row[0]
            patch_content = row[1]

            for hunks in self.get_commit_data(patch_content):
                # Get the file ID from the database for linking
                file_id_query = """select f.id from files f, actions a, scmlog s
                where a.commit_id = ?
                and a.file_id = f.id
                and f.file_name = ?"""
                
                # The regex strips the path from the file name, as per
                # cvsanaly convention
                read_cursor_1.execute(statement(file_id_query, db.place_holder), \
                        (commit_id, re.search("[^\/]*$", hunks.file_name).group(0)))
                print "File name: " + hunks.file_name + " File ID: " + str(read_cursor_1.fetchone()[0])

        read_cursor.close()
        read_cursor_1.close()
        connection.close()

        # This turns off the profiler and deletes it's timings
        profiler_stop("Running hunks extension", delete=True)

register_extension("Hunks", Hunks)
