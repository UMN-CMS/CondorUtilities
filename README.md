# Condor Utilities

These scripts allow easier scheduling of jobs on Condor that use `cmsRun`.

## condor_filelist.perl

`condor_filelist.perl` requires `batch_cmsRun` to be installed in `~/bin/`.

This script takes as input a CMSSW configuration file and a text file listing
the location of CMSSW ROOT data files. It then splits the file list into
chunks, creates versions of the configuration file to run on each chunks of
files, and schedules these jobs to run on Condor.

The most basic usage is:

    condor_filelist.perl cfg.py filelist.txt

Where `cfg.py` is a CMSSW configuration file, and `filelist.txt` is a text file
listing the absolute locations of a set of data files with one file listed per
line.

If run without arguments, `condor_filelist.perl` will produce the following
help text:

    Usage: [BASE CONFIG] [NAME OF FILE CONTAINING LIST OF FILENAMES] 

        --batch (number of files per jobs) (default 10)
        --start (output file number for first job) (default 0)
        --jobname (name of the job) (default based on base config)
        --prodSpace (production space) (default /local/cms/user/$USER)
        --nosubmit (don't actually submit, just make files)

These extra arguments are optional. They have the following function:

* `--batch` allows changing the number of data files to run per job.
* `--start` sets the starting number for the first output file.
* `--jobname` sets the name of the job, which is also used as the name of the
  directory created to store the results.
* `--prodSpace` sets the location of the output directory. The directory
  created to store the output files will be created in the location specified
  with this flag.
* `--nosubmit` prevents the jobs from being submitted to Condor; the
  configuration files that would be submitted are still generated though.
