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
#       Chris Lewis <cflewis@soe.ucsc.edu>

from pycvsanaly2.extensions import Extension, register_extension, \
        ExtensionRunError
from pycvsanaly2.utils import printdbg, printerr, printout, \
        remove_directory, uri_to_filename
from pycvsanaly2.profile import profiler_start, profiler_stop

class Content(Extension):
    def run(self, repo, url, db):
        profiler_start("Running content extension")
        printout("Hello extension world!")
        profiler_stop("Running content extension", delete=True)

register_extension("Content", Content)
