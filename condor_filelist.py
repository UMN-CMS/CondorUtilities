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
from os.path import isfile, isdir, basename
from sys import exit
from math import ceil
from subprocess import call


class CondorFile:
    """ Generates a file to submit to condor """
    def __init__(self, condorFile, executable, logDir, niceUser):
        self.condorFile = condorFile
        self.executable = executable
        self.logDir = logDir
        self.niceUser = niceUser
        self.__setHeader()
        self.cont = self.header

    def __setHeader(self):
        """ Just a way to serperate the hardcoded strings from everything else.
        """
        # List of machines not to run on
        self.banned_machines = [
            "zebra01.spa.umn.edu",
            "zebra02.spa.umn.edu",
            "zebra03.spa.umn.edu",
            "zebra04.spa.umn.edu",
            "caffeine.spa.umn.edu",
            "gc1-ce.spa.umn.edu",
            "gc1-hn.spa.umn.edu",
            "gc1-se.spa.umn.edu",
            "red.spa.umn.edu",
            "hadoop-test.spa.umn.edu"
            ]

        # Set up our string
        if self.niceUser:
            self.header = "nice_user = True\n"
        self.header = "Executable = %(executable)s\n" % {"executable": self.executable}
        self.header += "Universe = vanilla\n"
        self.header += "Output = %(logDir)s/output\n" % {"logDir": self.logDir}
        self.header += "Error = %(logDir)s/error\n" % {"logDir": self.logDir}
        self.header += "request_memory = 400\n"
        self.header += 'Requirements = (Arch=="X86_64")'
        # Insert banned machines
        for machine in self.banned_machines:
            self.header += ' && (Machine != "%s")' % machine
        self.header += '\n\n+CondorGroup="cmsfarm"\n\n'

    def addJob(self, scramArch, localRT, jobDir, cfgFile, logFile, elogFile, outputRootFile, sleep, firstInputFile):
        """ Add an 'Arguments' and a 'Queue' command to the condorfile. """
        self.cont += "# Job to run on %(cfgFile)s\n" % {"cfgFile": cfgFile}
        self.cont += "Arguments = %(scramArch)s %(localRT)s %(jobDir)s %(cfgFile)s %(logFile)s %(elogFile)s %(outputRootFile)s %(sleep)s %(firstInputFile)s\n" % {
            "scramArch": scramArch,
            "localRT": localRT,
            "jobDir": jobDir,
            "cfgFile": cfgFile,
            "logFile": logFile,
            "elogFile": elogFile,
            "outputRootFile": outputRootFile,
            "sleep": sleep,
            "firstInputFile": firstInputFile
            }
        self.cont += "Queue\n\n"

    def write(self):
        """ Save the condor file """
        # Check save location
        saveLocation = self.condorFile
        # Save file
        f = open(saveLocation, 'w')
        f.write(self.cont)
        f.flush()
        f.close()


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
            # Parse files in the "/store/" location
            if '/store/' in line.lower():
                loc = line.find('/store/')
                line = line[loc:]  # Keep everything from /store/ onward
                if line not in files:
                    files.append(line)
            # Otherwise make sure they exist, and then add file:
            elif isfile(line):
                line = "file:" + line
                if line not in files:
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

    def pop(self, n=1):
        """ Allow removal and return of an element. A list of the elements is
        always returned, even if only one element is asked for. """
        # Act like normal pop if multiple items are not desired
        if n <= 1:
            return [self.files.pop()]
        else:
            returnList = []
            for i in xrange(n):
                try:
                    returnList.append(self.files.pop())
                except IndexError:  # No items left
                    return returnList
            return returnList


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
    niceUser = False

    # Command line parsing
    from optparse import OptionParser

    usage = "usage: %prog -f FILELIST -b BASECONFIG [Options]"
    version = "%prog Version Beta 1\n\nCopyright (C) 2013 Alexander Gude - gude@physics.umn.edu\nThis is free software.  You may redistribute copies of it under the terms of\nthe GNU General Public License <http://www.gnu.org/licenses/gpl.html>.\nThere is NO WARRANTY, to the extent permitted by law.\n\nWritten by Alexander Gude."
    parser = OptionParser(usage=usage, version=version)
    parser.add_option("-f", "--file-list", action="store", type="str", dest="fileList", help="an input file containing a list of files to run over, with one file per line")
    parser.add_option("-b", "--base-config", action="store", type="str", dest="baseConfig", help="base config file for the job")
    parser.add_option("-p", "--prod-space", action="store", type="str", dest="prodSpace", default=prodSpace, help="the place to save the files [default %s]" % prodSpace)
    parser.add_option("-j", "--job-name", action="store", type="str", dest="jobName", default=jobName, help="the name for the set of jobs [default based on base config]")
    parser.add_option("-n", "--batch", action="store", type="int", dest="nBatch", default=nBatch, help="the number of files to run as one job [default 10]")
    parser.add_option("-s", "--start-point", action="store", type="int", dest="startPoint", default=startPoint, help="the file to start with [default 0]")
    parser.add_option("--no-submit", action="store_false", dest="submitJobs", default=submitJobs, help="prevent the script from submitting jobs to condor, only make files [default false]")
    parser.add_option("--nice", action="store_true", dest="niceUser", default=niceUser, help="only run jobs if no other jobs are waiting for spots [default false]")

    (options, args) = parser.parse_args()

    # Set up a default name based on the base config name
    if options.jobName is None:
        if "_cfg" in options.baseConfig:
            options.jobName = basename(options.baseConfig.split("_cfg")[0])
        else:
            options.jobName = basename(options.baseConfig.split('.')[0])

    # Set up directories
    jobDir = options.prodSpace + '/' + options.jobName + '/'
    logDir = jobDir + "log/"  # For logs from each job
    cfgDir = jobDir + "cfg/"
    condorDir = jobDir + "condor/"
    condorLogDir = condorDir + "log/"
    condorFile = condorDir + options.jobName + ".condor"
    # Make directories if they do not already exist
    for d in [jobDir, logDir, cfgDir, condorDir, condorLogDir]:
        if not isdir(d):
            makedirs(d)

    # Open files
    cf = CondorFile(condorFile, executable, logDir, options.niceUser)
    cfg = CFGFile(options.baseConfig)
    filelist = FileList(options.fileList)

    # Estimate the number of output files and prepare the base filename for
    # formatting
    # -1 in the following line because we start from 0, so 100 only needs 0-99
    nOutput = int(ceil(len(filelist) / options.nBatch)) - 1
    zeroPad = len(str(nOutput))
    formatString = "%%0%dd" % zeroPad  # %% is the literal %
    baseFileName = options.jobName + '_' + "%s" % formatString

    # Loop over files
    i = 0
    while len(filelist.files) > 0:
        # Add number to file name
        baseNumberedFileName = baseFileName % i
        # Set output files
        outputLogFile = jobDir + baseNumberedFileName + ".log"
        outputErrLogFile = jobDir + baseNumberedFileName + ".err"
        outputRootFile = jobDir + baseNumberedFileName + ".root"
        cfg.addOutputRootFile(outputRootFile)
        # Set input files
        inputFiles = filelist.pop(options.nBatch)
        cfg.addInputRootFiles(inputFiles)
        # Write cfg
        outputCFG = cfgDir + baseNumberedFileName + "_cfg.py"
        cfg.write(outputCFG)
        # Add job to condor file
        cf.addJob(
                scramArch, localRT, jobDir, outputCFG, outputLogFile,
                outputErrLogFile, outputRootFile, i * 2, inputFiles[0]
                )
        # Update counter
        i += 1

    # Write Condor file
    cf.write()

    # Submit Condor file
    if options.submitJobs:
        retcode = call(["condor_submit", condorFile])
        if retcode != 0:
            print "Error returned from condor_submit!"
            exit(retcode)
