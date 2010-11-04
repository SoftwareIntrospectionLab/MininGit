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
import os
import re

# This class holds a single repository retrieve task,
# and keeps the source code until the object is garbage-collected

class Hunks(Extension):
    deps = ['FilePaths','Patches']

    # Returns a list of dictionaries indicating the diff
    # contents. Each element of the list is a file, each
    # dictionary contains index and content attributes
    def split_diff(diff_string):
        diff_groups = re.findall(r"Index:\s+(\S+)\s*\n=*\s*\n(.*)Index:", diff_string, re.DOTALL)

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
            
        queuesize = int(os.getenv("CVSANALY_THREADS", 10))
        printdbg("Setting queuesize to " + str(queuesize))
        
        # Get the patches from this repository
        query = "select p.patch from patches p, scmlog s " + \
                "where p.commit_id = s.id and " + \
                "s.repository_id = ?"
        read_cursor.execute(statement(query, db.place_holder),(repo_id,))

        for row in read_cursor:
            print str(row[0])

        read_cursor.close()
        connection.close()

        # This turns off the profiler and deletes it's timings
        profiler_stop("Running hunks extension", delete=True)

register_extension("Hunks", Hunks)
