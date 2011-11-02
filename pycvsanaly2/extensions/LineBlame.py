# Copyright (C) 2011 University of California, Santa Cruz
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
from pycvsanaly2.Database import (SqliteDatabase, MysqlDatabase,
    TableAlreadyExists, statement)
from pycvsanaly2.extensions import register_extension

class LineBlameJob(BlameJob):
    class ContentHandler(BlameJob.BlameContentHandler):
        def __init__(self):
            self.hunks = []
            self.current_hunk = Hunk()
            
        def line(self, line):
            if self.current_hunk.rev != line.rev:
                self.current_hunk = Hunk(line.line, line.rev)
                self.hunks.append(self.current_hunk)
            else:
                self.current_hunk.end = line.line
                
    def __init__(self, file_id, commit_id, path, rev):
        BlameJob.__init__(self, file_id, commit_id, path, rev)
        
    def get_content_handler(self):
        return self.ContentHandler()

    def collect_results(self, content_handler):
        self.hunks = content_handler.hunks
        
        
class Hunk:
    def __init__(self, start=0, rev=''):
        self.start = start
        self.rev = rev
        self.end = start
        
class LineBlame(Blame):
    # Insert query
    __insert__ = """INSERT INTO line_blames (file_id, commit_id, start, 
                                       end, blame_commit_id)
                         VALUES (?,?,?,?,?)"""
    job_class = LineBlameJob
                         
    def create_table(self, cnn):
        cursor = cnn.cursor()
        if isinstance(self.db, SqliteDatabase):
            import sqlite3.dbapi2

            try:
                cursor.execute("""CREATE TABLE line_blames (
                                file_id integer,
                                commit_id integer,
                                start integer,
                                end integer,
                                blame_commit_id integer
                                )""")
            except sqlite3.dbapi2.OperationalError:
                cursor.close()
                raise TableAlreadyExists
            except:
                raise
        elif isinstance(self.db, MysqlDatabase):
            import MySQLdb
            
            try:
                cursor.execute("""CREATE TABLE line_blames (
                                id integer primary key auto_increment,
                                file_id integer,
                                commit_id integer,
                                start integer,
                                end integer,
                                blame_commit_id integer
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

    def populate_insert_args(self, job):
        args = []
        cnn = self.db.connect()
        cursor = cnn.cursor()
        for h in job.hunks:
            query = "select id from scmlog where rev = ?"
            cursor.execute(statement(query, self.db.place_holder),
                           (h.rev,))

            fetched_row = cursor.fetchone()

            if fetched_row is not None:
                args.append((job.file_id, job.commit_id, h.start, h.end, fetched_row[0]))
            
        cursor.close()
        cnn.close()
        return args

    def get_blames(self, cursor, repoid):
        query = "select distinct b.file_id, b.commit_id from line_blames b, files f " + \
                "where b.file_id = f.id and repository_id = ?"
        cursor.execute(statement(query, self.db.place_holder), (repoid,))
        return [(res[0], res[1]) for res in cursor.fetchall()]

    
    def backout(self, repo, uri, db):
        update_statement = """delete from lineblames where
                              commit_id in (select s.id from scmlog s
                                          where s.repository_id = ?)"""

        self._do_backout(repo, uri, db, update_statement)

register_extension("LineBlame", LineBlame)
