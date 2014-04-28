#!/usr/bin/env python

#  Copyright (C) 2014  Alexander Gude - gude@physics.umn.edu
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software Foundation,
#  Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
#
#  The most recent version of this program is available at:
#  https://github.com/UMN-CMS/CondorUtilities

from distutils.spawn import find_executable
from os import listdir, devnull
from os.path import isfile, isdir, basename, dirname, split
from subprocess import call
from sys import argv

##### START OF CODE
if __name__ == '__main__':

    # Check if zgrep exists
    if find_executable("zgrep") is None:
        print "Can not find zgrep."
        exit(2)

    # Get the input directory
    input_dir = argv[1]
    if not isdir(input_dir):
        exit("Input dir is invalid!")
    cfg_dir = split(dirname(input_dir))[0] + "/cfg/"

    # Loop over the err.gz files and look for "FileOpenError". If found, add
    # the file to a bad_files list.
    command = ["zgrep", "FileOpenError"]
    bad_files = []
    err_files = [input_dir + "/" + f for f in listdir(input_dir) if (isfile(input_dir + "/" + f) and f.endswith("err.gz"))]
    for err_file in err_files:
        args = command + [err_file]
        retcode = call(args, stdout=open(devnull, "wb"))
        # 0 is found, 1 is not found, 2 is read error
        if retcode == 0: 
            bad_files.append(basename(err_file))

    # Convert the bad files to a list of cfg files to rerun, complete with full
    # path
    for bad_file in bad_files:
        cfg_file = bad_file.split(".err.gz")[0] + "_cfg.py"
        if isfile(cfg_dir + cfg_file):
            print cfg_dir + cfg_file, 
