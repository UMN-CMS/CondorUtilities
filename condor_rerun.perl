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
print(SUBMIT "request_memory = 400\n");
print(SUBMIT "Requirements = (Arch==\"X86_64\")");
# Zebras are for remote login, not cluster computing
print(SUBMIT " && (Machine != \"zebra01.spa.umn.edu\" && Machine != \"zebra02.spa.umn.edu\" && Machine != \"zebra03.spa.umn.edu\" && Machine != \"zebra04.spa.umn.edu\" && Machine != \"caffeine.spa.umn.edu\")");
# These machines are VMs that run the grid interface
print(SUBMIT " && (Machine != \"gc1-ce.spa.umn.edu\" && Machine != \"gc1-hn.spa.umn.edu\" && Machine != \"gc1-se.spa.umn.edu\" && Machine != \"red.spa.umn.edu\" && Machine != \"hadoop-test.spa.umn.edu\")");
print(SUBMIT "\n");
print(SUBMIT "+CondorGroup=\"cmsfarm\"\n");

$sleep=2;

foreach $cfg (@ARGV) {

    $cfg=$ENV{"PWD"}."/".$cfg;
    $loc=$cfg;
    $loc=~s|cfg/.*||;

    $log=$cfg;
    $log=~s|cfg/|log/|;
    $log=~s|_cfg.py|.log|;

    $elog=$cfg;
    $elog=~s|cfg/|log/|;
    $elog=~s|_cfg.py|.err|;
    
    $scramarch=$ENV{"SCRAM_ARCH"};


    print(SUBMIT "Arguments = $scramarch $rt $loc $cfg $log $elog /tmp/nonesuch $sleep\n");
    print(SUBMIT "Queue\n");    

    $sleep=$sleep+2;
}

close(SUBMIT);
