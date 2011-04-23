# Copyright (C) 2006-2009 LibreSoft
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
#       Alvaro Navarro <anavarro@gsyc.escet.urjc.es>
#       Carlos Garcia Campos <carlosgc@gsyc.escet.urjc.es>

"""
Main funcion of cvsanaly. Fun starts here!

@author:       Alvaro Navarro, Carlos Garcia Campos
@organization: LibreSoft
@copyright:    LibreSoft (C) 2006-2009 LibreSoft
@license:      GNU GPL version 2 or any later version
@contact:      libresoft-tools-devel@lists.morfeo-project.org
"""

import os
import getopt
from repositoryhandler.Command import Command, CommandTimeOut
from repositoryhandler.backends import (create_repository, 
    create_repository_from_path, RepositoryUnknownError)
from ParserFactory import (create_parser_from_logfile, 
    create_parser_from_repository)
from Database import (create_database, TableAlreadyExists, AccessDenied, 
    DatabaseNotFound, DatabaseDriverNotSupported, DBRepository, statement, 
    initialize_ids, DatabaseException)
from DBProxyContentHandler import DBProxyContentHandler
from Log import LogReader, LogWriter
from extensions import get_all_extensions
from ExtensionsManager import (ExtensionsManager, InvalidExtension, 
    InvalidDependency)
from Config import Config, ErrorLoadingConfig
from utils import printerr, printout, uri_to_filename, printdbg
from _config import *
from DBDeletionHandler import DBDeletionHandler


def usage():
    print "%s %s - %s" % (PACKAGE, VERSION, DESCRIPTION)
    print COPYRIGHT
    print
    print "Usage: cvsanaly2 [options] [URI]"
    print """
Analyze the given URI. An URI can be a checked out directory, 
or a remote URL pointing to a repository. If URI is omitted,
the current working directory will be used as a checked out directory.

Options:

  -h, --help                     Print this usage message.
  -V, --version                  Show version
  -g, --debug                    Enable debug mode
  -q, --quiet                    Run silently, only print error messages
      --profile                  Enable profiling mode
  -f, --config-file              Use a custom configuration file
  -l, --repo-logfile=path        Logfile to use instead of getting log from 
                                 the repository
  -s, --save-logfile[=path]      Save the repository log to the given path
  -n, --no-parse                 Skip the parsing process. It only makes sense 
                                 in conjunction with --extensions
      --extensions=ext1,ext2,    List of extensions to run        
      --hard-order               Execute extensions in exactly the order given. 
                                 Won't follow extension dependencies.
      --branch=[branch]          Specify branch that should be monitored (only
                                 works for Git right now)
      --low-memory               Changes cvsanaly to store certain caches on
                                 the hard drive. This is not well-supported,
                                 only use if you are having out-of-memory
                                 problems (usually because your repo is very
                                 large). This may also cause cvsanaly to run
                                 slower, as it is not caching in memory, and
                                 is somewhat paranoid about the integrity of
                                 the on-disk cache, so will often empty it.

Database:

      --db-driver                Output database driver [mysql|sqlite] (mysql)
  -u, --db-user                  Database user name (operator)
  -p, --db-password              Database user password
  -d, --db-database              Database name (cvsanaly)
  -H, --db-hostname              Name of the host where database server 
                                 is running (localhost)

Metrics Options:

      --metrics-all              Get metrics for every revision, not only 
                                 for HEAD
      --metrics-noerr            Ignore errors when calculating metrics
      
Content options:
      --no-content               When running the Content extension, don't 
                                 insert the content (ie. you just want the
                                 lines of code count)
File Count options:
      --count-types=type1,type2  When running the File Count extension, only
                                 count the types (based on regex in
                                 extensions/file_types.py)
"""


def main(argv):
    # Short (one letter) options. Those requiring argument followed by :
    short_opts = "hVgqbnf:l:s:u:p:d:H:"
    # Long options (all started by --). Those requiring argument followed by =
    long_opts = ["help", "version", "debug", "quiet", "profile", 
                 "config-file=", "repo-logfile=", "save-logfile=", 
                 "no-parse", "db-user=", "db-password=", "db-hostname=", 
                 "db-database=", "db-driver=", "extensions=", "hard-order", 
                 "metrics-all", "metrics-noerr", "no-content", "branch=",
                 "backout", "low-memory", "count-types="]

    # Default options
    debug = None
    quiet = None
    profile = None
    configfile = None
    no_parse = None
    user = None
    passwd = None
    hostname = None
    database = None
    driver = None
    logfile = None
    save_logfile = None
    extensions = None
    metrics_all = None
    metrics_noerr = None
    hard_order = None
    low_memory = None
    no_content = None
    branch = None
    backout = None
    count_types = None

    try:
        opts, args = getopt.getopt(argv, short_opts, long_opts)
    except getopt.GetoptError, e:
        printerr(str(e))
        return 1

    for opt, value in opts:
        if opt in("-h", "--help", "-help"):
            usage()
            return 0
        elif opt in("-V", "--version"):
            print VERSION
            return 0
        elif opt in("--debug", "-g"):
            debug = True
        elif opt in("--quiet", "-q"):
            quiet = True
        elif opt in("--profile", ):
            profile = True
        elif opt in("--no-parse", "-n"):
            no_parse = True
        elif opt in("-f", "--config-file"):
            configfile = value
        elif opt in("-u", "--db-user"):
            user = value
        elif opt in("-p", "--db-password"):
            passwd = value
        elif opt in("-H", "--db-hostname"):
            hostname = value
        elif opt in("-d", "--db-database"):
            database = value
        elif opt in("--db-driver"):
            driver = value
        elif opt in("-l", "--repo-logfile"):
            logfile = value
        elif opt in("-s", "--save-logfile"):
            save_logfile = value
        elif opt in("--extensions", ):
            extensions = value.split(',')
        elif opt in("--count-types", ):
            count_types = value.split(',')
        elif opt in("--hard-order"):
            hard_order = True
        elif opt in("--low-memory"):
            low_memory = True
        elif opt in("--branch"):
            branch = value
        elif opt in("--metrics-all", ):
            metrics_all = True
        elif opt in("--metrics-noerr", ):
            metrics_noerr = True
        elif opt in ("--no-content", ):
            no_content = True
        elif opt in ("-b", "--backout"):
            backout = True

    if len(args) <= 0:
        uri = os.getcwd()
    else:
        uri = args[0]

    config = Config()
    try:
        if configfile is not None:
            config.load_from_file(configfile)
        else:
            config.load()
    except ErrorLoadingConfig, e:
        printerr(e.message)
        return 1

    if debug is not None:
        config.debug = debug
    if quiet is not None:
        config.quiet = quiet
    if profile is not None:
        config.profile = profile
    if logfile is not None:
        config.repo_logfile = logfile
    if save_logfile is not None:
        config.save_logfile = save_logfile
    if no_parse is not None:
        config.no_parse = no_parse
    if driver is not None:
        config.db_driver = driver
    if user is not  None:
        config.db_user = user
    if passwd is not None:
        config.db_password = passwd
    if hostname is not None:
        config.db_hostname = hostname
    if database is not None:
        config.db_database = database
    if extensions is not None:
        config.extensions.extend([item for item in extensions \
                                  if item not in config.extensions])
    if count_types is not None:
        config.count_types.extend([item for item in count_types \
                                  if item not in config.count_types])
    if hard_order is not None:
        config.hard_order = hard_order
    if low_memory is not None:
        config.low_memory = low_memory
    if branch is not None:
        config.branch = branch
    if metrics_all is not None:
        config.metrics_all = metrics_all
    if metrics_noerr is not None:
        config.metrics_noerr = metrics_noerr
    if no_content is not None:
        config.no_content = no_content
    if backout is not None:
        config.extensions = get_all_extensions()

    if not config.extensions and config.no_parse:
        # Do nothing!!!
        return 0

    if config.debug:
        import repositoryhandler.backends
        repositoryhandler.backends.DEBUG = True

    # Create repository
    path = uri_to_filename(uri)
    if path is not None:
        try:
            printdbg("Creating repositoryhandler instance")
            repo = create_repository_from_path(path)
            repo.timeout = 120
        except RepositoryUnknownError:
            printerr("Path %s doesn't seem to point to a repository " + \
                     "supported by cvsanaly", (path,))
            return 1
        except Exception, e:
            printerr("Unknown error creating repository for path %s (%s)", 
                     (path, str(e)))
            return 1
        uri = repo.get_uri_for_path(path)
    else:
        uri = uri.strip('/')
        repo = create_repository('svn', uri)
        # Check uri actually points to a valid svn repo
        if repo.get_last_revision(uri) is None:
            printerr("URI %s doesn't seem to point to a valid svn repository", 
                     (uri,))
            return 1

    if not config.no_parse:
        printdbg("Preparing logging")
        # Create reader
        reader = LogReader()
        reader.set_repo(repo, path or uri)
        reader.set_branch(config.branch)

        # Create parser
        if config.repo_logfile is not None:
            parser = create_parser_from_logfile(config.repo_logfile)
            reader.set_logfile(config.repo_logfile)
        else:
            parser = create_parser_from_repository(repo)

        parser.set_repository(repo, uri)

        if parser is None:
            printerr("Failed to create parser")
            return 1

        # TODO: check parser type == logfile type
    
    db_exists = False

    try:
        printdbg("Creating database")
        db = create_database(config.db_driver,
                             config.db_database,
                             config.db_user,
                             config.db_password,
                             config.db_hostname)
    except AccessDenied, e:
        printerr("Error creating database: %s", (e.message,))
        return 1
    except DatabaseNotFound:
        printerr("Database %s doesn't exist. It must be created before " + \
                 "running cvsanaly", (config.db_database,))
        return 1
    except DatabaseDriverNotSupported:
        printerr("Database driver %s is not supported by cvsanaly", 
                 (config.db_driver,))
        return 1
    
    try:
        printdbg("Starting ExtensionsManager")
        emg = ExtensionsManager(config.extensions, 
                                hard_order=config.hard_order)
    except InvalidExtension, e:
        printerr("Invalid extension %s", (e.name,))
        return 1
    except InvalidDependency, e:
        printerr("Extension %s depends on extension %s which is not a " + \
                 "valid extension", (e.name1, e.name2))
        return 1
    except Exception, e:
        printerr("Unknown extensions error: %s", (str(e),))
        return 1
    
    cnn = db.connect()
        
    if backout:
        # Run extensions
        #printout(str(get_all_extensions()))
        printout("Backing out all extensions")
        emg.backout_extensions(repo, path or uri, db)
        printout("Backing out repo from database")
        backout_handler = DBDeletionHandler(db, repo, uri, cnn)
        backout_handler.begin()
        
        # Final commit just in case
        cnn.commit()
        cnn.close()
        return 1
    
    cursor = cnn.cursor()
    
    try:
        printdbg("Creating tables")
        db.create_tables(cursor)
        cnn.commit()
    except TableAlreadyExists:
        printdbg("Tables not created, database already exists")
        db_exists = True
    except DatabaseException, e:
        printerr("Database error: %s", (e.message,))
        return 1

    if config.no_parse and not db_exists:
        printerr("The option --no-parse must be used with an already " + \
                 "filled database")
        return 1

    # Add repository to Database
    if db_exists:
        printdbg("Database exists, so looking for existing repository")
        cursor.execute(statement("SELECT id from repositories where uri = ?", 
                                 db.place_holder), (uri,))
        rep = cursor.fetchone()
        initialize_ids(db, cursor)
        cursor.close()

    if config.no_parse and rep is None:
        printerr("The option --no-parse must be used with an already " + \
                 "filled database")
        return 1
        
    if not db_exists or rep is None:
        # We consider the name of the repo as the last item of the root path
        name = uri.rstrip("/").split("/")[-1].strip()
        cursor = cnn.cursor()
        rep = DBRepository(None, uri, name, repo.get_type())
        cursor.execute(statement(DBRepository.__insert__, db.place_holder), 
                       (rep.id, rep.uri, rep.name, rep.type))
        cursor.close()
        cnn.commit()

    cnn.close()

    if not config.no_parse:
        # Start the parsing process
        printout("Parsing log for %s (%s)", (path or uri, repo.get_type()))
        
        def new_line(line, user_data):
            parser, writer = user_data
        
            parser.feed(line)
            writer and writer.add_line(line)
        
        writer = None
        if config.save_logfile is not None:
            writer = LogWriter(config.save_logfile)
        
        parser.set_content_handler(DBProxyContentHandler(db))
        reader.start(new_line, (parser, writer))
        parser.end()
        writer and writer.close()

    # Run extensions
    printout("Executing extensions")
    emg.run_extensions(repo, path or uri, db)
