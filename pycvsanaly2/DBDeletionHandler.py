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
                      DBTag, DBTagRev)
from utils import printdbg

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
    
    def __init__(self, db, repo, uri):
        self.db = db
        self.repo = repo
        self.uri = uri
        
        # Getting the repo ID should be put in the database class
        # and the extensions also refactored to use it
        self.repo_id = True
    
    def begin(self):
        self.delete_tags()
        self.delete_tag_revisions()
        self.delete_file_copies()
        self.delete_branches()
        self.delete_actions()
        self.delete_people()
        self.delete_file_links()
        self.delete_files()
        self.delete_log()
        self.delete_repo()
        
    def do_delete(self, statement, params=None):
        # You can't reference instance variables in default
        # parameters, so I have to do this.
        if params is None:
            params = (self.repo_id,)
        
        
    def delete_tags(self):
        # Delete tags by looking at tag_revisions attached to commits in this
        # repo
        printdbg("Deleting tags")
        delete_statement = """DELETE FROM tags
            WHERE id IN (SELECT tr.id 
                             FROM tag_revisions tr, scmlog s
                             WHERE tr.commit_id = s.id
                             AND s.repository_id = ?)
        """
        
    def delete_tag_revisions(self):
        printdbg("Deleting tag revisions")
        delete_statement = """DELETE FROM tag_revisions
            WHERE commit_id IN (SELECT s.id 
                                FROM scmlog s
                                WHERE s.repository_id = ?)
        """
            
    def delete_file_copies(self):
        printdbg("Deleting file copies")
        delete_statement = """DELETE FROM file_copies
            WHERE action_id IN (SELECT a.id 
                                FROM actions a, scmlog s
                                WHERE a.commit_id = s.id
                                AND s.repository_id = ?)
        """
        
    def delete_branches(self):
        printdbg("Deleting branches")
        delete_statement = """DELETE from branches
            WHERE id IN (SELECT a.branch_id
                         FROM actions a, scmlog s
                         WHERE a.commit_id = s.id
                         AND s.repository_id = ?)
        """
        
    def delete_actions(self):
        printdbg("Deleting actions")
        delete_statement = """DELETE FROM actions
            WHERE commit_id IN (SELECT s.id
                                FROM scmlog s
                                WHERE s.repository_id = ?)
        """
    
    def delete_people(self):
        printdbg("Deleting people")
        delete_statement = """DELETE FROM people
            WHERE id IN (SELECT s.author_id
                         FROM scmlog s
                         WHERE s.repository_id = ?
                         union
                         SELECT s.committer_id
                         FROM scmlog s
                         WHERE s.repository_id = ?)
        """
        
    def delete_file_links(self):
        printdbg("Deleting file links")
        delete_statement = """DELETE FROM file_links
            WHERE commit_id IN (SELECT s.id
                                FROM scmlog s
                                WHERE s.repository_id = ?)
        """
        
    def delete_files(self):
        printdbg("Deleting files")
        delete_statement = """DELETE FROM files
            WHERE repository_id = ?
        """
        
    def delete_log(self):
        printdbg("Deleting commit log")
        delete_statement = """DELETE FROM scmlog
            WHERE repository_id = ?
        """
        
    def delete_repo(self):
        printdbg("Deleting repository")
        delete_statement = """DELETE FROM repositories
            WHERE repository_id = ?
        """