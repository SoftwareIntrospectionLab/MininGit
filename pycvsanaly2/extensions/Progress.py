# Copyright (C) 2011 Alexander Pepper
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
#       Alexander Pepper <pepper@inf.fu-berlin.de>

import sys
import time
from progressbar import Percentage, Bar, RotatingMarker, ETA, ProgressBar

class Progress(object):
    def __init__(self, label, max_value):
        self.nr_done = 0
        widgets = [label, ': ', Percentage(), ' ', Bar(marker=RotatingMarker()),
                       ' ', ETA()]
        self.pbar = ProgressBar(widgets=widgets, maxval=max_value)
        self.pbar.start()

    def done(self):
        self.pbar.finish()

    def finished_one(self):
        self.nr_done += 1
        self.pbar.update(self.nr_done)
