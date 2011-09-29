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
#    Carlos Garcia Campos <carlosgc@gsyc.escet.urjc.es>
#    Zhongpeng Lin <zlin5@ucsc.edu>

from repositoryhandler.backends.watchers import DIFF
from repositoryhandler.Command import CommandError, CommandRunningError
from pycvsanaly2.Database import (SqliteDatabase, MysqlDatabase,
        TableAlreadyExists, statement, ICursor, execute_statement)
from pycvsanaly2.profile import profiler_start, profiler_stop
from pycvsanaly2.Config import Config
from pycvsanaly2.extensions import (Extension, register_extension,
    ExtensionRunError)
from pycvsanaly2.utils import to_utf8, printerr, printdbg, uri_to_filename
from io import BytesIO
from Jobs import JobPool, Job
from pycvsanaly2.PatchParser import *
from pycvsanaly2.extensions.FilePaths import FilePaths


class PatchJob(Job):
    def __init__(self, rev, commit_id):
        self.rev = rev
        self.commit_id = commit_id
        self.data = None

    def get_patch_for_commit(self):
        def diff_line(data, io):
            io.write(data)

        io = BytesIO()
        wid = self.repo.add_watch(DIFF, diff_line, io)

        done = False
        failed = False
        retries = 3

        while not done and not failed:
            try:
                self.repo.show(self.repo_uri, self.rev)
                self.data = to_utf8(unicode(to_utf8(io.getvalue()), "utf-8", errors='replace')).strip()
                done = True
            except (CommandError, CommandRunningError) as e:
                if retries > 0:
                    printerr("Error running show command: %s, trying again",
                             (str(e),))
                    retries -= 1
                    io.seek(0)
                elif retries <= 0:
                    failed = True
                    printerr("Error running show command: %s, FAILED",
                             (str(e),))
                    self.data = None

        self.repo.remove_watch(DIFF, wid)

        return self.data

    def run(self, repo, repo_uri):
        profiler_start("Processing patch for revision %s", (self.rev))
        self.repo = repo
        self.repo_uri = repo_uri
        self.get_patch_for_commit()
        profiler_stop("Processing patch for revision %s", (self.rev))


class DBPatch(object):

    __insert__ = """INSERT INTO patches (commit_id, file_id, patch)
                    values (?, ?, ?)"""

    def __init__(self, db, commit_id, data):
        self.db = db;
        self.commit_id = commit_id
        self.data = data
        self.fp = FilePaths(self.db)
        
    def file_patches(self):
        lines = [l+"\n" for l in self.data.split("\n") if l]
        
        cnn = self.db.connect()
        for f in iter_file_patch(lines, True):
            try:
                patch = parse_patch(f.__iter__(), allow_dirty=True)
            except PatchSyntax, BinaryFiles:
                continue
            file_name = patch.file_name()
            file_id = self.fp.get_file_id(file_name, self.commit_id)
            
            if file_id is None:
                printerr("File id for %s @  %s not found" % (file_name, self.commit_id))
                continue
            else:
                yield file_id, patch
        cnn.close()

    def __str__(self):
        return "<commit_id: %s, data: %s>" % \
                (str(self.commit_id),
                 to_utf8(self.data).decode("utf-8"))


class Patches(Extension):

    INTERVAL_SIZE = 100

    def __init__(self):
        self.db = None

    def __create_table(self, cnn):
        cursor = cnn.cursor()

        if isinstance(self.db, SqliteDatabase):
            import sqlite3.dbapi2

            try:
                cursor.execute("""CREATE TABLE patches (
                                id integer primary key AUTOINCREMENT,
                                commit_id integer NOT NULL,
                                file_id integer NOT NULL,
                                patch text,
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
                cursor.execute("""CREATE TABLE patches (
                                id integer primary key auto_increment,
                                commit_id integer NOT NULL REFERENCES scmlog(id),
                                file_id integer NOT NULL REFERENCES files(id),
                                patch LONGTEXT,
                                UNIQUE(commit_id, file_id)
                                ) ENGINE=InnoDB, CHARACTER SET=utf8""")
            except MySQLdb.OperationalError, e:
                if e.args[0] == 1050:
                    cursor.close()
                    raise TableAlreadyExists
                raise
            except:
                raise

        cnn.commit()
        cursor.close()

    def __process_finished_jobs(self, job_pool, write_cursor, db):
        finished_job = job_pool.get_next_done()

        # scmlog_id is the commit ID. For some reason, the
        # documentation advocates tablename_id as the reference,
        # but in the source, these are referred to as commit IDs.
        # Don't ask me why!
        num_processed_jobs = 0
        while finished_job is not None:
            p = DBPatch(db, finished_job.commit_id, finished_job.data)
            
            for file_id, patch in p.file_patches():
#                printerr("Inserting patch for file %d at commit %d" % (file_id, p.commit_id))
                execute_statement(statement(DBPatch.__insert__,
                                            self.db.place_holder),
                                  (p.commit_id, file_id, str(patch)),
                                  write_cursor,
                                  db,
                                  "Couldn't insert, duplicate patch?",
                                  exception=ExtensionRunError)
            num_processed_jobs += 1
            finished_job = job_pool.get_next_done(0)
            
        return num_processed_jobs

    def run(self, repo, uri, db):
        profiler_start("Running Patches extension")
        self.db = db
        self.repo = repo

        path = uri_to_filename(uri)
        if path is not None:
            repo_uri = repo.get_uri_for_path(path)
        else:
            repo_uri = uri

        path = uri_to_filename(uri)
        self.repo_uri = path or repo.get_uri()

        cnn = self.db.connect()

        cursor = cnn.cursor()
        write_cursor = cnn.cursor()
        cursor.execute(statement("SELECT id from repositories where uri = ?",
                                 db.place_holder), (repo_uri,))
        repo_id = cursor.fetchone()[0]

        try:
            printdbg("Creating patches table")
            self.__create_table(cnn)
        except TableAlreadyExists:
            pass
        except Exception, e:
            raise ExtensionRunError(str(e))

        queuesize = Config().max_threads
        job_pool = JobPool(repo, path or repo.get_uri(), queuesize=queuesize)
        i = 0

        icursor = ICursor(cursor, self.INTERVAL_SIZE)
        icursor.execute(statement("SELECT id, rev, composed_rev " + \
                                  "from scmlog where repository_id = ?",
                                    db.place_holder), (repo_id,))
        rs = icursor.fetchmany()

        while rs:
            for commit_id, revision, composed_rev in rs:
                if composed_rev:
                    rev = revision.split("|")[0]
                else:
                    rev = revision

                job = PatchJob(rev, commit_id)
                job_pool.push(job)

                i = i + 1
                if i >= queuesize:
                    printdbg("Queue is now at %d, flushing to database", (i,))
                    num_processed_jobs = self.__process_finished_jobs(job_pool, write_cursor, db)
                    i -= num_processed_jobs
                    if num_processed_jobs < queuesize/5:
                        job_pool.join()
                        
            cnn.commit()
            rs = icursor.fetchmany()

        job_pool.join()
        self.__process_finished_jobs(job_pool, write_cursor, db)
        cnn.commit()
        write_cursor.close()
        cursor.close()
        cnn.close()
        profiler_stop("Running Patches extension", delete=True)

    def backout(self, repo, uri, db):
        update_statement = """delete from patches
                              where commit_id in (select s.id from scmlog s
                                          where s.repository_id = ?)"""

        self._do_backout(repo, uri, db, update_statement)

register_extension("Patches", Patches)
