#!/usr/bin/perl

#------------------------


$executable=$ENV{"HOME"}."/bin/batch_cmsRun";
$rt=$ENV{"LOCALRT"};

if (length($rt)<2) {
	print "You must run eval `scramv1 runtime -csh` in the right release\n";
        print "before running this script!\n";
	exit(1);
}

#------------------------

#open(SUBMIT,">condor_submit.txt");
open(SUBMIT,"|condor_submit");
print(SUBMIT "Executable = $executable\n");
print(SUBMIT "Universe = vanilla\n");
#print(SUBMIT "Requirements = Memory > 400 && (Arch==\"INTEL\" || Arch==\"X86_64\")");
print(SUBMIT "Requirements = Memory > 400 && (Arch==\"X86_64\")");
#print(SUBMIT " && (Machine == \"zebra01.spa.umn.edu\" || Machine == \"zebra02.spa.umn.edu\" || Machine == \"zebra03.spa.umn.edu\" || Machine == \"zebra04.spa.umn.edu\")");
print(SUBMIT " && (Machine != \"zebra04.spa.umn.edu\")");
print(SUBMIT "\n");
print(SUBMIT "+CondorGroup=\"cmsfarm\"\n");

$sleep=2;

foreach $cfg (@ARGV) {

    $loc=$cfg;
    $loc=~s|cfg/.*||;

    $log=$cfg;
    $log=~s|cfg/|log/|;
    $log=~s|_cfg.py|.log|;

    $elog=$cfg;
    $elog=~s|cfg/|log/|;
    $elog=~s|_cfg.py|.err|;


    print(SUBMIT "Arguments = $rt $loc $cfg $log $elog /tmp/nonesuch $sleep\n");
    print(SUBMIT "Queue\n");    

    $sleep=$sleep+2;
}

close(SUBMIT);
