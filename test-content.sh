#!/usr/bin/env bash
rm test-db.sqlite
./cvsanaly2 -g --db-driver sqlite --db-database test-db.sqlite --extensions=Content https://svn.forge.morfeo-project.org/svn/libresoft-tools