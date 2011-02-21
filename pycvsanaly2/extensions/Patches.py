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

from pycvsanaly2.Database import (SqliteDatabase, MysqlDatabase, TableAlreadyExists,
                                  statement, ICursor, execute_statement)
from pycvsanaly2.Config import Config
from pycvsanaly2.extensions import Extension, register_extension, ExtensionRunError
from pycvsanaly2.utils import to_utf8, printerr, printdbg, uri_to_filename
from io import BytesIO
from Jobs import JobPool, Job

class PatchJob(Job):
    def __init__(self, rev, commit_id):
        self.rev = rev
        self.commit_id = commit_id
        self.data = None

    def get_patch_for_commit (self):
        def diff_line (data, io):
            io.write (data)

        io = BytesIO ()
        wid = self.repo.add_watch (DIFF, diff_line, io)
        try:
            self.repo.show (self.repo_uri, self.rev)
            self.data = io.getvalue ()
        except Exception, e:
            printerr ("Error running show command: %s", (str(e),))
            self.data = None

        self.repo.remove_watch (DIFF, wid)
        io.close ()
        
        self.data = self.data.strip()

        return self.data

    def run(self, repo, repo_uri):
        self.repo = repo
        self.repo_uri = repo_uri
        self.get_patch_for_commit()


class DBPatch:

    id_counter = 1

    __insert__ = "INSERT INTO patches (id, commit_id, patch) values (?, ?, ?)"

    def __init__ (self, id, commit_id, data):
        if id is None:
            self.id = DBPatch.id_counter
            DBPatch.id_counter += 1
        else:
            self.id = id

        self.commit_id = commit_id
        self.patch = data

    def __str__(self):
        return "<Patch ID: " + str(self.id) + ", commit_id: " + str(self.commit_id) + \
                ", data: " + to_utf8(self.patch).decode("utf-8") + ">" 

class Patches (Extension):

    INTERVAL_SIZE = 100

    def __init__ (self):
        self.db = None

    def __create_table (self, cnn):
        cursor = cnn.cursor ()

        if isinstance (self.db, SqliteDatabase):
            import sqlite3.dbapi2

            try:
                cursor.execute ("CREATE TABLE patches (" +
                                "id integer primary key," +
                                "commit_id integer," +
                                "patch text" +
                                ")")
            except sqlite3.dbapi2.OperationalError:
                cursor.close ()
                raise TableAlreadyExists
            except:
                raise
        elif isinstance (self.db, MysqlDatabase):
            import _mysql_exceptions

            try:
                cursor.execute ("CREATE TABLE patches (" +
                                "id INT primary key," +
                                "commit_id integer," +
                                "patch LONGTEXT" +
                                # "FOREIGN KEY (commit_id) REFERENCES scmlog(id)" +
                                ") ENGINE=InnoDB, CHARACTER SET=utf8")
            except _mysql_exceptions.OperationalError, e:
                if e.args[0] == 1050:
                    cursor.close ()
                    raise TableAlreadyExists
                raise
            except:
                raise

        cnn.commit ()
        cursor.close ()

    def __get_patches_for_repository (self, repo_id, cursor):
        query = "SELECT p.commit_id from patches p, scmlog s " + \
                "WHERE p.commit_id = s.id and repository_id = ?"
        cursor.execute (statement (query, self.db.place_holder), (repo_id,))
        commits = [res[0] for res in cursor.fetchall ()]

        return commits

    def __process_finished_jobs(self, job_pool, write_cursor, db):
        finished_job = job_pool.get_next_done()

        # scmlog_id is the commit ID. For some reason, the 
        # documentaion advocates tablename_id as the reference,
        # but in the source, these are referred to as commit IDs.
        # Don't ask me why!
        while finished_job is not None:
            p = DBPatch (None, finished_job.commit_id, finished_job.data)

            execute_statement(statement(DBPatch.__insert__, self.db.place_holder),
                              (p.id, p.commit_id, to_utf8(p.patch).decode("utf-8")),
                              write_cursor,
                              db,
                              "Couldn't insert, duplicate patch?",
                              exception=ExtensionRunError)

            finished_job = job_pool.get_next_done(0)

    def run (self, repo, uri, db):
        self.db = db
        self.repo = repo

        path = uri_to_filename (uri)
        if path is not None:
            repo_uri = repo.get_uri_for_path (path)
        else:
            repo_uri = uri

        path = uri_to_filename (uri)
        self.repo_uri = path or repo.get_uri ()

        cnn = self.db.connect ()

        cursor = cnn.cursor ()
        cursor.execute (statement ("SELECT id from repositories where uri = ?", db.place_holder), (repo_uri,))
        repo_id = cursor.fetchone ()[0]

        # If table does not exist, the list of commits is empty,
        # otherwise it will be filled within the except block below
        commits = []

        try:
            self.__create_table (cnn)
        except TableAlreadyExists:
            cursor.execute (statement ("SELECT max(id) from patches", db.place_holder))
            id = cursor.fetchone ()[0]
            if id is not None:
                DBPatch.id_counter = id + 1

            commits = self.__get_patches_for_repository (repo_id, cursor)
        except Exception, e:
            raise ExtensionRunError (str (e))

        queuesize = Config().max_threads
        job_pool = JobPool(repo, path or repo.get_uri(), queuesize=queuesize)        
        i = 0

        write_cursor = cnn.cursor ()
        icursor = ICursor (cursor, self.INTERVAL_SIZE)
        icursor.execute (statement ("SELECT id, rev, composed_rev from scmlog where repository_id = ?",
                                    db.place_holder), (repo_id,))
        rs = icursor.fetchmany ()
        while rs:
            for commit_id, revision, composed_rev in rs:
                if commit_id in commits: 
                    continue

                if composed_rev:
                    rev = revision.split ("|")[0]
                else:
                    rev = revision

                job = PatchJob(rev, commit_id)
                job_pool.push(job)

                i = i + 1
                if i >= queuesize:
                    printdbg("Queue is now at %d, flushing to database", (i,))
                    job_pool.join()
                    self.__process_finished_jobs(job_pool, write_cursor, db)
                    cnn.commit()
                    i = 0

            cnn.commit()
            rs = icursor.fetchmany ()

        job_pool.join()
        self.__process_finished_jobs(job_pool, write_cursor, db)
        cnn.commit ()
        write_cursor.close ()
        cursor.close ()
        cnn.close ()

register_extension ("Patches", Patches)
