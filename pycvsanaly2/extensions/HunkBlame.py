# Copyright (C) 2010 University of California, Santa Cruz
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
#       Zhongpeng Lin  <zlin5@ucsc.edu>

from Blame import BlameJob, Blame
from pycvsanaly2.extensions import register_extension, ExtensionRunError
from pycvsanaly2.profile import profiler_start, profiler_stop
from pycvsanaly2.utils import printdbg, printerr, uri_to_filename
from pycvsanaly2.Database import (SqliteDatabase, MysqlDatabase,
    TableAlreadyExists, statement)
from repositoryhandler.backends import RepositoryCommandError
from repositoryhandler.backends.watchers import BLAME
from guilty.parser import create_parser, ParserUnknownError
from Jobs import JobPool, Job
from FilePaths import FilePaths
from Progress import Progress
import os
import sys


class HunkBlameJob(Job):
    class BlameContentHandler(BlameJob.BlameContentHandler):
        def __init__(self, hunks):
            self.hunks = hunks
            self.bug_revs = {}

        def line(self, blame_line):
            for hunk_id, start_line, end_line in self.hunks:
                if blame_line.line >= start_line and \
                blame_line.line <= end_line:
                    if self.bug_revs.get(hunk_id) is None:
                        self.bug_revs[hunk_id] = set()
                    self.bug_revs[hunk_id].add(blame_line.rev)
                    break

        def start_file(self, filename):
            self.filename = filename
            printdbg("Processing blame output for %s",
                           (self.filename))

        def end_file(self):
            if len(self.bug_revs) == 0:
                printdbg("No bug revision found in this file")

    def __init__(self, hunks, current_path, current_rev):
        Job.__init__(self)
        self.prev_path = None
        self.prev_rev = None
        self.bug_revs = {}

        self.hunks = hunks
        self.current_path = current_path
        self.current_rev = current_rev

    def __do_the_blame(self, repo, repo_uri):
        printdbg("Running HunkBlameJob for %s@%s", (self.prev_path, self.prev_rev))

        def blame_line(line, p):
            p.feed(line)

        start = sys.maxint
        end = 0

        for hunk in self.hunks:
            if hunk[1] < start:
                start = hunk[1]
            if hunk[2] > end:
                end = hunk[2]

        repo_type = repo.get_type()
        if repo_type == 'cvs':
            # CVS paths contain the module stuff
            uri = repo.get_uri_for_path(repo_uri)
            module = uri[len(repo.get_uri()):].strip('/')

            if module != '.':
                path = self.prev_path[len(module):].strip('/')
            else:
                path = self.prev_path.strip('/')
        else:
            path = self.prev_path.strip('/')

        try:
            printdbg("Creating parser")
            p = create_parser(repo.get_type(), self.prev_path)
        except ParserUnknownError:
            printdbg("Parser not found, getting one from the repo.")
            # The parser isn't part of guilty.
            # This method lets a repo that isn't part of the
            # ecosystem to specifiy it's own blame parser
            p = repo.get_blame_parser(self.prev_path)

        out = self.get_content_handler()
        p.set_output_device(out)
        wid = repo.add_watch(BLAME, blame_line, p)
        try:
            repo.blame(os.path.join(repo_uri, self.prev_path), self.prev_rev,
                       start=start, end=end)
            self.collect_results(out)
        except RepositoryCommandError, e:
            self.failed = True
        p.end()
        repo.remove_watch(BLAME, wid)

        return not self.failed

    def run(self, repo, repo_uri):
        try:
            try:
                prev_tuple = repo.get_previous_commit_and_file_name(repo_uri, self.current_rev, self.current_path)
            except NotImplementedError:
                raise ExtensionRunError("HunkBlame extension is not supported " + \
                                        "for %s repositories" % (repo.get_type()))

            if prev_tuple is None or prev_tuple == "":
                raise NotValidHunkWarning(
                    """Couldn't find previous path for file %s@%s""" % (self.current_path, self.current_rev))

            (self.prev_rev, self.prev_path) = prev_tuple

            self.__do_the_blame(repo, repo_uri)
        except NotValidHunkWarning as e:
            printerr("Not a valid hunk: " + str(e))

    def get_content_handler(self):
        return self.BlameContentHandler(self.hunks)

    def collect_results(self, content_handler):
        self.bug_revs = content_handler.bug_revs

    def get_bug_revs(self):
        return self.bug_revs


class NotValidHunkWarning(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)


class HunkBlame(Blame):

    deps = ['Hunks']

    MAX_BLAMES = 10

    # Insert query
    __insert__ = 'INSERT INTO hunk_blames (hunk_id, bug_commit_id) ' + \
                 'VALUES (?,?)'

    def __init__(self):
        #Only to conform the interface of superclass
        self.id_counter = 1

    def __create_table(self, cnn):
        cursor = cnn.cursor()

        if isinstance(self.db, SqliteDatabase):
            import sqlite3.dbapi2
            try:
                cursor.execute("CREATE TABLE hunk_blames (" +
                                "id integer primary key," +
                                "hunk_id integer," +
                                "bug_commit_id integer," +
                                "KEY hunk_id (hunk_id)"
                                ")")
            except sqlite3.dbapi2.OperationalError:
                raise TableAlreadyExists
            finally:
                cursor.close()
                
        elif isinstance(self.db, MysqlDatabase):
            import MySQLdb

            try:
                cursor.execute("CREATE TABLE hunk_blames (" +
                                "id integer primary key auto_increment," +
                                "hunk_id integer REFERENCES hunks(id)," +
                                "bug_commit_id integer REFERENCES scmlog(id)," +
                                "KEY hunk_id (hunk_id)" +
                                ") CHARACTER SET=utf8")
            except MySQLdb.OperationalError, e:
                if e.args[0] == 1050:
                    raise TableAlreadyExists
                else:
                    raise
            except:
                raise
            finally:
                cursor.close()
        cnn.commit()

    def __add_index(self, cnn):
        cursor = cnn.cursor()
        query = "CREATE INDEX rev ON scmlog(rev(40));"
        if isinstance(self.db, SqliteDatabase):
            import sqlite3.dbapi2
            try:
                cursor.execute(query)
            except sqlite3.dbapi2.OperationalError:
                pass
            finally:
                cursor.close()
                
        elif isinstance(self.db, MysqlDatabase):
            import MySQLdb
            try:
                cursor.execute(query)
            except MySQLdb.OperationalError, e:
                if e.args[0] != 1061:
                    raise
            finally:
                cursor.close()
        

    def __drop_cache(self, cnn):
        cursor = cnn.cursor()

        if isinstance(self.db, SqliteDatabase):
            import sqlite3.dbapi2
            try:
                cursor.execute("drop table _action_files_cache")
            except sqlite3.dbapi2.OperationalError:
                # Do nothing, thats OK
                pass
            finally:
                cursor.close()
        elif isinstance(self.db, MysqlDatabase):
            import MySQLdb

            try:
                cursor.execute("drop table _action_files_cache")
            except MySQLdb.OperationalError, e:
                if e.args[0] != 1050:
                    raise
            finally:
                cursor.close()

    def __create_cache(self, cnn):
        cursor = cnn.cursor()

        try:
            self.__drop_cache(cnn)
        except Exception, e:
            printdbg("Couldn't drop cache because of " + str(e))

        if isinstance(self.db, SqliteDatabase):
            import sqlite3.dbapi2
            try:
                cursor.execute("""CREATE TABLE _action_files_cache as
                    select * from action_files""")
            except sqlite3.dbapi2.OperationalError:
                raise TableAlreadyExists
            finally:
                cursor.close()
        elif isinstance(self.db, MysqlDatabase):
            import MySQLdb

            try:
                cursor.execute("""CREATE TABLE _action_files_cache as
                    select * from action_files""")
            except MySQLdb.OperationalError, e:
                if e.args[0] == 1050:
                    raise TableAlreadyExists
                else:
                    raise
            finally:
                cursor.close()

        cnn.commit()

    def __get_hunk_blames(self, cursor, repoid):
        query = """select distinct b.hunk_id
            from hunk_blames b
            join hunks h on b.hunk_id=h.id
            join files f on h.file_id=f.id
            where f.repository_id=?"""
        cursor.execute(statement(query, self.db.place_holder), (repoid,))
        return [h[0] for h in cursor.fetchall()]

    def populate_insert_args(self, job):
        bug_revs = job.get_bug_revs()
        cnn = self.db.connect()
        cursor = cnn.cursor()
        args = []
        for hunk_id in bug_revs:
            for rev in bug_revs[hunk_id]:
                printdbg("Find id for rev %s" % rev)
                query = "select id from scmlog where rev = ?"
                cursor.execute(statement(query, self.db.place_holder),
                               (rev,))

                fetched_row = cursor.fetchone()

                if fetched_row is not None:
                    args.append((hunk_id, fetched_row[0]))

        cursor.close()
        cnn.close()
        return args

    def run(self, repo, uri, db):
        profiler_start("Running HunkBlame extension")

        self.db = db
        self.uri = uri
        self.fp = FilePaths(self.db)

        cnn = self.db.connect()
        read_cursor = cnn.cursor()
        write_cursor = cnn.cursor()
        try:
            path = uri_to_filename(uri)
            if path is not None:
                repo_uri = repo.get_uri_for_path(path)
            else:
                repo_uri = uri

            read_cursor.execute(statement("SELECT id from repositories " + \
                    "where uri = ?", db.place_holder), (repo_uri,))
            repoid = read_cursor.fetchone()[0]
        except NotImplementedError:
            raise ExtensionRunError("HunkBlame extension is not supported " + \
                                    "for %s repositories" % (repo.get_type()))
        except Exception, e:
            raise ExtensionRunError("Error creating repository %s. " + \
                                    "Exception: %s" % (repo.get_uri(), str(e)))

        try:
            self.__create_table(cnn)
        except TableAlreadyExists:
            pass
        except Exception, e:
            raise ExtensionRunError(str(e))

        try:
            self.__create_cache(cnn)
        except TableAlreadyExists:
            pass
        except Exception, e:
            raise ExtensionRunError(str(e))
        
        self.__add_index(cnn)

        blames = self.__get_hunk_blames(read_cursor, repoid)

        job_pool = JobPool(repo, path or repo.get_uri(), queuesize=100)

        outer_query = """select distinct h.file_id, h.commit_id
            from hunks h, scmlog s
            where h.commit_id=s.id and s.repository_id=?
                and h.old_start_line is not null
                and h.old_end_line is not null
                and h.file_id is not null
                and h.commit_id is not null
        """
        read_cursor.execute(statement(outer_query, db.place_holder), (repoid,))
        progress = Progress("[Extension HunkBlame]", read_cursor.rowcount)
        file_rev = read_cursor.fetchone()
        n_blames = 0
        while file_rev is not None:
            try:
                file_id, commit_id = file_rev

                # get current file_path
                current_path = self.fp.get_path_from_database(file_id, commit_id)
                if current_path is None:
                    raise NotValidHunkWarning(
                        """Couldn't find path for file ID %d at commit ID %d"""
                        % (file_id, commit_id))

                # get current revision
                rev_query = """SELECT rev FROM scmlog WHERE id = ?"""
                try:
                    write_cursor.execute(statement(rev_query, self.db.place_holder),
                                   (commit_id,))
                    current_rev = write_cursor.fetchone()[0]
                except:
                    raise NotValidHunkWarning(
                        """Couldn't find revision for commit ID %d"""
                        % (commit_id))

                # get all hunks for file and revision
                try:
                    inner_cursor = cnn.cursor()
                    inner_query = """select h.id, h.old_start_line,
                            h.old_end_line from hunks h
                            where h.file_id = ? and h.commit_id = ?
                            and h.old_start_line is not null
                            and h.old_end_line is not null
                            and h.file_id is not null
                            and h.commit_id is not null
                    """
                    inner_cursor.execute(statement(inner_query,
                                                   db.place_holder),
                                                   (file_id, commit_id))
                    hunks = inner_cursor.fetchall()
                except:
                    raise NotValidHunkWarning(
                        """Couldn't find hunks for file ID %d at commit ID %d"""
                        % (file_id, commit_id))
                finally:
                    inner_cursor.close()
                hunks = [h for h in hunks if h[0] not in blames]

                # create the Job and run it
                job = HunkBlameJob(hunks, current_path, current_rev)
                job_pool.push(job)
                n_blames += 1

                if n_blames >= self.MAX_BLAMES:
                    processed_jobs = self.process_finished_jobs(job_pool,
                                                                 write_cursor)
                    n_blames -= processed_jobs

                    if processed_jobs <= (self.MAX_BLAMES / 5):
                        printdbg("Joining unprocessed jobs")
                        job_pool.join()

            except NotValidHunkWarning as e:
                printerr("Not a valid hunk: " + str(e))
            finally:
                file_rev = read_cursor.fetchone()
                progress.finished_one()

        job_pool.join()
        self.process_finished_jobs(job_pool, write_cursor, True)

        try:
            self.__drop_cache(cnn)
        except:
            printdbg("Couldn't drop cache because of " + str(e))

        self.fp.close()
        read_cursor.close()
        write_cursor.close()
        cnn.close()
        progress.done()

        profiler_stop("Running HunkBlame extension", delete=True)

    def backout(self, repo, uri, db):
        update_statement = """delete from hunk_blames where
                              hunk_id in (select h.id from hunks h, scmlog s
                                          where h.commit_id = s.id
                                          and s.repository_id = ?)"""

        self._do_backout(repo, uri, db, update_statement)

register_extension("HunkBlame", HunkBlame)
