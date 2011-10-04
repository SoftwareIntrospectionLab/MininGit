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

from pygments.lexers import get_lexer_for_filename, guess_lexer, TextLexer
from pygments.util import ClassNotFound
from repositoryhandler.backends.watchers import CAT
from repositoryhandler.Command import CommandError, CommandRunningError
from pycvsanaly2.utils import to_utf8, printerr, printdbg
from io import BytesIO
import os

def _convert_linebreaks(input):
    """Converts all linebreaks (e.g. from windows) to one format"""

    # source: http://code.activestate.com/recipes/435882-normalizing-newlines-between-windowsunixmacs/
    return input.replace('\r\n', '\n').replace('\r', '\n')

def _strip_lines(text):
    """Strip every line of whitespaces."""

    text_array = map(lambda s: s.strip(), text.split("\n"))
    return "\n".join(text_array)

def _get_file_content(repo, uri, rev):
    """Reads the content of a file and revision from a given repository"""

    def write_line(data, io):
        io.write(data)

    io = BytesIO()
    wid = repo.add_watch(CAT, write_line, io)
    try:
        repo.cat(uri, rev)
        file_content = to_utf8(io.getvalue()).decode("utf-8")
        file_content = _convert_linebreaks(file_content) #make shure we do have the same new lines.
    except Exception as e:
        printerr("[get_line_types] Error running show command: %s, FAILED", (str(e),))
        file_content = None

    repo.remove_watch(CAT, wid)
    return file_content

def _iterate_lexer_output(iterator):
    """Iterate Lexer Output and build an array from it.
       Each item in Array is another Array which represents a line"""

    output_lines = []
    output_line = []
    for ttype, value in iterator:
        input_lines = value.split("\n")
        for i in range(len(input_lines)):
            item = {}
            item["token"] = str(ttype)
            item["value"] = to_utf8(input_lines[i]).decode("utf-8")
            if (item["value"] != '') or (i == 0):
                output_line.append(item)
            if (len(input_lines) > 1) and (i < len(input_lines)-1):
                output_lines.append(output_line)
                output_line = []
    return output_lines

def _comment_empty_or_code(lines_array):
    """Decides what type a line is.
       Possible values:
       * code - if excecutable code
       * comment - a nonexecutable comment
       * empty - an empty line (or only containing whitespaces)"""

    output = ""
    for line in lines_array:
        if (len(line) < 1):
            output += "empty\n"
            continue
        first_token = line[0]["token"]
        first_value = line[0]["value"]
        if (len(line) == 1) & (first_token == "Token.Text") & (first_value == ""):
            output += "empty\n"
        elif (len(line) == 1) & ((first_token == "Token.Comment.Single") | (first_token == "Token.Comment.Multiline")):
            output += "comment\n"
        else:
            output += "code\n"

    return output

def get_line_types(repo, repo_uri, rev, path):
    """Returns an array, where each item means a line of code.
       Each item is labled 'code', 'comment' or 'empty'"""

    #profiler_start("Processing LineTypes for revision %s:%s", (self.rev, self.file_path))
    uri = os.path.join(repo_uri, path) # concat repo_uri and file_path for full path
    file_content = _get_file_content(repo, uri, rev)  # get file_content

    if file_content is None or file_content == '':
        printerr("[get_line_types] Error: No file content for " + str(rev) + ":" + str(path) + " found! Skipping.")
        line_types = None
    else:
        try:
            lexer = get_lexer_for_filename(path)
        except ClassNotFound:
            try:
                printdbg("[get_line_types] Guessing lexer for" + str(rev) + ":" + str(path) + ".")
                lexer = guess_lexer(file_content)
            except ClassNotFound:
                printdbg("[get_line_types] No guess or lexer found for " + str(rev) + ":" + str(path) + ". Using TextLexer instead.")
                lexer = TextLexer()

        # Not shure if this should be skipped, when the language uses off-side rules (e.g. python,
        # see http://en.wikipedia.org/wiki/Off-side_rule for list)
        stripped_code = _strip_lines(file_content)
        lexer_output = _iterate_lexer_output(lexer.get_tokens(stripped_code))
        line_types_str = _comment_empty_or_code(lexer_output)
        line_types = line_types_str.split("\n")

    return line_types
    #profiler_stop("Processing LineTypes for revision %s:%s", (self.rev, self.file_path))

def line_is_code(line_types_array, line_nr):
    """Decides if a given line nr is executable code"""

    try:
        line_type = line_types_array[line_nr-1]
    except IndexError as e:
        printdbg("Line lexer output. Must be an empty line!")
        line_type = None

    return line_type == "code"
