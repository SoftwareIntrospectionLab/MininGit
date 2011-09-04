# Copyright (C) 2009 LibreSoft
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
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
#       Carlos Garcia Campos <carlosgc@gsyc.escet.urjc.es>

from repositoryhandler.backends.watchers import DIFF
from repositoryhandler.Command import CommandError, CommandRunningError
from pycvsanaly2.Database import (SqliteDatabase, MysqlDatabase,
        TableAlreadyExists, statement, ICursor, execute_statement)
from pycvsanaly2.profile import profiler_start, profiler_stop
from pycvsanaly2.Config import Config
from pycvsanaly2.extensions import (Extension, register_extension,
    ExtensionRunError)
from pycvsanaly2.extensions.Hunks import Hunks
from pycvsanaly2.extensions.Patches import PatchJob, DBPatch
from pycvsanaly2.utils import to_utf8, printerr, printdbg, uri_to_filename
from io import BytesIO
from Jobs import JobPool, Job

class PatchesAndHunks(Extension):
    """An extension to insert hunks without the intermediate patches step."""
    INTERVAL_SIZE = 100

    def __init__(self):
        self.db = None

    def run(self, repo, uri, db):
        def patch_generator(repo, repo_uri, repo_id, db, cursor):
            icursor = ICursor(cursor, self.INTERVAL_SIZE)
            icursor.execute(statement("SELECT id, rev, composed_rev " + \
                                      "from scmlog where repository_id = ?",
                                      db.place_holder), (repo_id,))

            rs = icursor.fetchmany()

            while rs:
                for commit_id, revision, composed_rev in rs:
                    # Get the patch
                    pj = PatchJob(revision, commit_id)

                    path = uri_to_filename(repo_uri)
                    pj.run(repo, path or repo.get_uri())

                    p = DBPatch(db, commit_id, pj.data)
                    # Yield the patch to hunks
                    for file_id, patch in p.file_patches():
                        yield (pj.commit_id, file_id, str(patch), pj.rev)

                rs = icursor.fetchmany()


        profiler_start("Running PatchesAndHunks extension")

        hunks = Hunks()
        hunks.get_patches = patch_generator
        hunks.run(repo, uri, db)

register_extension("PatchesAndHunks", PatchesAndHunks)
