#!/usr/bin/python

#  Copyright (C) 2013  Alexander Gude - gude@physics.umn.edu
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
#  The most recent version of this program is avaible at:
#  https://github.com/UMN-CMS/CondorUtilities


from os import environ, makedirs
from os.path import isfile, isdir
from sys import exit

Requirements = """
Executable = %(executable)s
Universe = vanilla
Output = %(logDir)s/output
Error = %(logDir)s/error
request_memory = 400
Requirements = (Arch==\"X86_64\")
&& (Machine != \"zebra01.spa.umn.edu\")
&& (Machine != \"zebra02.spa.umn.edu\")
&& (Machine != \"zebra03.spa.umn.edu\")
&& (Machine != \"zebra04.spa.umn.edu\")
&& (Machine != \"caffeine.spa.umn.edu\")
&& (Machine != \"gc1-ce.spa.umn.edu\")
&& (Machine != \"gc1-hn.spa.umn.edu\")
&& (Machine != \"gc1-se.spa.umn.edu\")
&& (Machine != \"red.spa.umn.edu\")
&& (Machine != \"hadoop-test.spa.umn.edu\")

+CondorGroup=\"cmsfarm\"

"""


class CFGFile:
    """ Handles reading, writing, and parsing of a CFGFile """
    def __init__(self, cfgFile):
        self.cfgFile = cfgFile
        self.__readFile()
        self.tab = "    "  # Tabs ARE spaces

    def __readFile(self):
        """ Read in contents of self.cfgFile """
        f = open(self.cfgFile)
        cont = f.read()
        f.close()
        self.cont = cont.splitlines()

    def write(self, saveLocation=None):
        """ Save the cfg file. The default location is the file that was read in. """
        # Check save location
        if saveLocation is None:
            saveLocation = self.cfgFile
        # Save file
        f = open(saveLocation, 'w')
        f.write('\n'.join(self.cont))
        f.flush()
        f.close()

    def addInputRootFiles(self, inputFiles):
        """ Add a rootfile or list of rootfiles to add to the CFG as input
        files. Each time this function is called, it overwrites previous
        inputfiles. """

        # Set up lines to insert
        inputs = [self.tab + "fileNames = cms.untracked.vstring("]
        if isinstance(inputFiles, basestring):  # Check if inputFiles is a string, or unicode string
            inputs.append(2 * self.tab + '"%s"' % inputFiles)
        else:
            # For each file (except for the last one) add it with a comma
            for inputFile in inputFiles[:-1]:
                inputs.append(2 * self.tab + '"%s",' % inputFile)
            # For the last file, do not put a comma
            inputs.append(2 * self.tab + '"%s"' % inputFiles[-1])

        inputs.append(self.tab + "),")

        # Get paren positions
        (openParenLine, closeParenLine) = self.__returnParenLocation("PoolSource", "fileNames")

        # This is list slicing combined with list addition
        #
        # We do not add 1 to the openParenLine number because slicing stops at
        # the entry before you tell it to stop, and we replace this line.
        #
        # We add 1 to the closeParenLine because we want to skip the paren
        # because it is included in the inputs list
        self.cont = self.cont[:openParenLine] + inputs + self.cont[closeParenLine + 1:]

    def addOutputRootFile(self, outputFile):
        """ Add the root files stored in self.rootFiles to the cfgFile to be
        run over """

        # Set up lines to insert
        inputs = [self.tab + "fileName = cms.string(", 2 * self.tab + '"%s"' % outputFile, self.tab + "),"]

        # Get paren positions
        (openParenLine, closeParenLine) = self.__returnParenLocation("TFileService", "fileName")

        # This is list slicing combined with list addition
        self.cont = self.cont[:openParenLine] + inputs + self.cont[closeParenLine + 1:]

    def __returnParenLocation(self, moduleName, variableName):
        """ Given a moduleName and an variableName, returns the opening and
        closing line numbers of the parens """
        inModule = False
        inVariableName = False
        openParenLine = None  # Set to None until we find a value
        closeParenLine = None

        for i in xrange(len(self.cont)):
            line = self.cont[i]

            # Flag when we are inside the module
            if moduleName in line:
                inModule = True

            # Flag when we are inside the variable list
            if inModule and variableName in line:
                inVariableName = True

            # Find the opening paren
            if inVariableName and '(' in line:
                openParenLine = i
                if ')' in line:  # Case when all files are given on one line
                    closeParenLine = i

                if closeParenLine:
                    break

            # If not already found, find the close paren
            if openParenLine and ')' in line:
                closeParenLine = i
                break

        return (openParenLine, closeParenLine)


class FileList:
    """ Handles a list of files """
    def __init__(self, filelist):
        """ Set up the class """
        self.filelist = filelist
        self.__parseList()

    def __parseList(self):
        """ Parse a filelist, and make a list of all valid unique files """
        # Open filelist for reading
        files = []
        f = open(self.filelist)
        cont = f.readlines()
        f.close()

        # Store only valid unique files
        for line in cont:
            line = line.strip()  # Remove white space and trailing \n
            if isfile(line) and line not in files:
                files.append(line)

        self.files = files

    def __len__(self):
        """ Return length of self.files """
        return self.files.__len__()

    def __getslice__(self, i, j):
        """ Allow slicing """
        return self.files.__getslice(i, j)

    def __iter__(self):
        """ Allow iteration over the list """
        return self.files.__iter__()

    def pop(self):
        """ Allow removal and return of an element """
        return self.files.pop()


# Only Runs Interactively
if __name__ == '__main__':

    # Check for critical environment variables, exit with an error if we don't
    # find them
    try:
        localRT = environ["LOCALRT"]
    except KeyError:
        exit("$LOCALRT not set. Remember to run 'cmsenv' in the right release area.")
    try:
        scramArch = environ["SCRAM_ARCH"]
    except KeyError:
        exit("$SCRAM_ARCH not set. Remember to run 'cmsenv' in the right release area.")

    # Check that batch_cmsRun exists
    try:
        executable = environ["HOME"] + "/bin/batch_cmsRun"
    except KeyError:
        exit("$HOME not set, so batch_cmsRun not found.")
    else:
        if not isfile(executable):
            exit("Can not find $HOME/bin/batch_cmsRun. Please make sure it exists.")

    # Set Defaults, these are used to set the defaults in the add_options calls
    nBatch = 10
    startPoint = 0
    submitJobs = True
    prodSpace = "/local/cms/user/" + environ["USER"]
    jobName = None

    # Command line parsing
    from optparse import OptionParser

    usage = "usage: %prog -f FILELIST -b BASECONFIG [Options]"
    version = "%prog Version 1.0.0\n\nCopyright (C) 2013 Alexander Gude - gude@physics.umn.edu\nThis is free software.  You may redistribute copies of it under the terms of\nthe GNU General Public License <http://www.gnu.org/licenses/gpl.html>.\nThere is NO WARRANTY, to the extent permitted by law.\n\nWritten by Alexander Gude."
    parser = OptionParser(usage=usage, version=version)
    parser.add_option("-f", "--file-list", action="store", type="str", dest="fileList", help="an input file containing a list of files to run over, with one file per line")
    parser.add_option("-b", "--base-config", action="store", type="str", dest="baseConfig", help="base config file for the job")
    parser.add_option("-p", "--prod-space", action="store", type="str", dest="prodSpace", default=prodSpace, help="the place to save the files [default %s]" % prodSpace)
    parser.add_option("-j", "--job-name", action="store", type="str", dest="jobName", default=jobName, help="the name for the set of jobs [default based on base config]")
    parser.add_option("-n", "--batch", action="store", type="int", dest="nBatch", default=nBatch, help="the number of files to run as one job [default 10]")
    parser.add_option("-s", "--start-point", action="store", type="int", dest="startPoint", default=startPoint, help="the file to start with [default 0]")
    parser.add_option("--no-submit", action="store_false", dest="submitJobs", default=submitJobs, help="prevent the script from submitting jobs to condor, only make files [default false]")

    (options, args) = parser.parse_args()

    # Parse the list of files
    filelist = FileList(options.fileList)

    # Set up a default name based on the base config name
    if options.jobName is None:
        if "_cfg" in options.baseConfig:
            options.jobName = options.baseConfig.split('_cfg')[0]
        else:
            options.jobName = options.baseConfig.split('.')[0]

    # Set up directories
    jobDir = options.prodSpace + '/' + options.jobName + '/'
    logDir = jobDir + 'log/'
    cfgDir = jobDir + 'cfg/'
    dirs = [jobDir, logDir, cfgDir]
    # Make directories if they do not already exist
    for d in dirs:
        if not isdir(d):
            makedirs(d)

    # print Requirements % { "logDir" : logDir, "executable" : executable }
