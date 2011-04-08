# Copyright (C) 2011 Regents of the University of California, Santa Cruz
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
#       Chris Lewis <cflewis@soe.ucsc.edu>

from Database import (DBRepository, DBLog, DBFile, DBFileLink,
                      DBPerson, DBBranch, DBAction, DBFileCopy,
                      DBTag, DBTagRev, execute_statement, get_repo_id)
from utils import printdbg, printout

class DBDeletionHandler:
    """A class for deleting a repository's information from a repository.
    
    FAQ: 
    
    Q: Why does this have a bunch of SQL embedded in it? Isn't it
    better to use the __delete__ statements for each DBobject?
    
    A: It's quicker and less fault-prone to have the SQL backend do the
    heavy lifting of the delete when there are a number of IDs associated.
    Instead of querying for the IDs, getting them back, having Python
    convert them, just so we can then issue a bunch *more* SQL statements,
    we can just do it in one with subqueries. This reduces overhead, plus
    makes the code easier to follow.
    """
    
    def __init__(self, db, repo, uri, connection):
        self.db = db
        self.repo = repo
        self.uri = uri
        self.connection = connection
        
        cursor = self.connection.cursor()
        self.repo_id = get_repo_id(self.uri, cursor, self.db)
        cursor.close()
    
    def begin(self):
        statements = (
            ("tags", """DELETE FROM tags
                       WHERE id IN (SELECT tr.id 
                                    FROM tag_revisions tr, scmlog s
                                    WHERE tr.commit_id = s.id
                                    AND s.repository_id = ?)
                    """),
            ("tag_revisions", """DELETE FROM tag_revisions
                                WHERE commit_id IN (SELECT s.id 
                                                    FROM scmlog s
                                                    WHERE s.repository_id = ?)
                             """),
            ("file_copies", """DELETE FROM file_copies
                              WHERE action_id IN (SELECT a.id 
                                                  FROM actions a, scmlog s
                                                  WHERE a.commit_id = s.id
                                                  AND s.repository_id = ?)
                           """),
            ("branches", """DELETE from branches
                           WHERE id IN (SELECT a.branch_id
                                        FROM actions a, scmlog s
                                        WHERE a.commit_id = s.id
                                        AND s.repository_id = ?)
                        """),
            ("actions", """DELETE FROM actions
                          WHERE commit_id IN (SELECT s.id
                                              FROM scmlog s
                                              WHERE s.repository_id = ?)
                       """),
            ("authors", """DELETE FROM people
                          WHERE id IN (SELECT s.author_id
                                       FROM scmlog s
                                       WHERE s.repository_id = ?)
                       """),
            ("committers", """DELETE FROM people
                             WHERE id IN (SELECT s.committer_id
                                          FROM scmlog s
                                          WHERE s.repository_id = ?)
                          """),
            ("file_links", """DELETE FROM file_links
                             WHERE commit_id IN (SELECT s.id
                                                 FROM scmlog s
                                                 WHERE s.repository_id = ?)
                          """),
            ("files", """DELETE FROM files WHERE repository_id = ?"""),
            ("commit log", """DELETE FROM scmlog WHERE repository_id = ?"""),
            ("repository", """DELETE FROM repositories WHERE id = ?""")
        )
        
        for (data_name, statement) in statements:
            printout("Deleting " + data_name)
            self.do_delete(statement)
        
        self.connection.commit()
        
    def do_delete(self, delete_statement, params=None,
                  error_message = "Delete failed, data will need manual cleanup"):
        # You can't reference instance variables in default
        # parameters, so I have to do this.
        if params is None:
            params = (self.repo_id,)
        
        try:
            delete_cursor = self.connection.cursor()
            execute_statement(delete_statement, params, delete_cursor,
                              self.db, error_message)
        except Exception:
            printdbg("Deletion exception")
        finally:
            delete_cursor.close()
        
        
    