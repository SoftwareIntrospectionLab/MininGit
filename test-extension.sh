#!/usr/bin/env bash
rm test-db.sqlite
./cvsanaly2 -g -u root -p root --extensions=$1 ~/Downloads/voldemort/