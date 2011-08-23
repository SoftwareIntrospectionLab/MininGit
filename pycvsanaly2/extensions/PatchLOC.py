# Copyright (C) 2011 Alexander Pepper
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
#       Alexander Pepper <pepper@inf.fu-berlin.de>

from pycvsanaly2.Database import SqliteDatabase, MysqlDatabase, statement, \
    ICursor, execute_statement, TableAlreadyExists
from pycvsanaly2.extensions import Extension, register_extension, \
    ExtensionRunError
from pycvsanaly2.utils import printdbg, printerr, printout, uri_to_filename, \
    to_utf8
from pycvsanaly2.profile import profiler_start, profiler_stop
import re

class PatchLOC(Extension):
    deps = ['Patches']
    INTERVAL_SIZE = 100
    patterns = {}
    patterns['old_file'] = re.compile("^--- a")
    patterns['no_old_file'] = re.compile("^--- \/dev\/null")
    patterns['new_file'] = re.compile("^\+\+\+ b")
    patterns['no_new_file'] = re.compile("^\+\+\+ \/dev\/null")
    patterns['added'] = re.compile("^\+.?")#"^\+(?!\+)"
    patterns['removed'] = re.compile("^-.?")#"^-(?!-)"

    def __init__(self):
        self.db = None

    def __create_table(self, cnn):
        cursor = cnn.cursor()

        if isinstance(self.db, SqliteDatabase):
            import sqlite3.dbapi2

            try:
                cursor.execute("""CREATE TABLE patch_lines (
                                id integer primary key AUTOINCREMENT,
                                commit_id integer NOT NULL,
                                file_id integer NOT NULL,
                                added integer,
                                removed integer,
                                UNIQUE(commit_id, file_id)
                                )""")
            except sqlite3.dbapi2.OperationalError:
                cursor.close()
                raise TableAlreadyExists
            except:
                raise
        elif isinstance(self.db, MysqlDatabase):
            import MySQLdb

            try:
                cursor.execute("""CREATE TABLE patch_lines (
                                id integer primary key auto_increment,
                                commit_id integer NOT NULL,
                                file_id integer NOT NULL,
                                added int,
                                removed int,
                                UNIQUE(commit_id, file_id)
                                ) CHARACTER SET=utf8""")
            except MySQLdb.OperationalError, e:
                if e.args[0] == 1050:
                    cursor.close()
                    raise TableAlreadyExists
                raise
            except:
                raise

        cnn.commit()
        cursor.close()

    def get_patches(self, repo, repo_uri, repo_id, db, cursor):
        icursor = ICursor(cursor, self.INTERVAL_SIZE)
        # Get the patches from this repository
        query = """select p.commit_id, p.file_id, p.patch, s.rev
                    from patches p, scmlog s
                    where p.commit_id = s.id and
                    s.repository_id = ? and
                    p.patch is not NULL"""
        icursor.execute(statement(query, db.place_holder), (repo_id,))
        rs = icursor.fetchmany()
        while rs:
            for commit_id, file_id, patch_content, rev in rs:
                yield (commit_id, file_id, unicode(to_utf8(patch_content), "utf-8"), rev)
            rs = icursor.fetchmany()

    def count_lines(self, patch_content):
        added = 0
        removed = 0

        for line in patch_content.splitlines():
            if self.patterns['old_file'].match(line) or self.patterns['no_old_file'].match(line):
                # skip line
                continue
            elif self.patterns['new_file'].match(line) or self.patterns['no_new_file'].match(line):
                # skip line
                continue
            elif self.patterns['added'].match(line):
                added += 1
            elif self.patterns['removed'].match(line):
                removed += 1

        return (added, removed)

    def run(self, repo, uri, db):
        profiler_start("Running PatchLOC extension")

        # Open a connection to the database and get cursors
        self.db = db
        connection = self.db.connect()
        cursor = connection.cursor()

        path = uri_to_filename(uri)
        if path is not None:
            repo_uri = repo.get_uri_for_path(path)
        else:
            repo_uri = uri

        cursor.execute(statement("SELECT id from repositories where uri = ?",
                                 db.place_holder), (repo_uri,))
        repo_id = cursor.fetchone()[0]

        try:
            self.__create_table(connection)
        except TableAlreadyExists:
            pass
        except Exception, e:
            raise ExtensionRunError(str(e))

        patches = self.get_patches(repo, path or repo.get_uri(), repo_id, db,
                                   cursor)

        for commit_id, file_id, patch_content, rev in patches:
            (added, removed) = self.count_lines(patch_content)
            insert = """insert into patch_lines(file_id, commit_id,
                        added, removed)
                        values(?,?,?,?)"""
            execute_statement(statement(insert, db.place_holder),
                              (file_id, commit_id, added, removed),
                               cursor,
                               db,
                               "Couldn't insert patch, dup record?",
                               exception=ExtensionRunError)
            connection.commit()

        cursor.close()
        connection.commit()
        connection.close()

        profiler_stop("Running PatchLOC extension", delete=True)

register_extension("PatchLOC", PatchLOC)
