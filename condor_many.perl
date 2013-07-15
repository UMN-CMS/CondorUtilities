#!/usr/bin/perl

#------------------------
#$prodSpace=$ENV{"HOME"}."/work";
$prodSpace="/local/cms/user/".$ENV{"USER"};

$executable=$ENV{"HOME"}."/bin/batch_cmsRun";
$rt=$ENV{"LOCALRT"};
$basecfg=shift @ARGV;

if (length($rt)<2) {
	print "You must run eval `scramv1 runtime -csh` in the right release\n";
        print "before running this script!\n";
	exit(1);
}

#------------------------

$cfg=$basecfg;

    $stub3=$cfg;
    $stub3=~s|.*/||g;
    $stub3=~s|_cfg.py||;

$jobBase=$ARGV[0];
$jobBase=~s|.*/||g;
$jobBase=~s|_[0-9]+.root||;
$jobBase=$stub3;
mkdir("$prodSpace/$jobBase");
mkdir("$prodSpace/$jobBase/cfg");
mkdir("$prodSpace/$jobBase/log");

$linearn=0;

srand(); # make sure rand is ready to go
#open(SUBMIT,">condor_submit.txt");
open(SUBMIT,"|condor_submit");
print(SUBMIT "Executable = $executable\n");
print(SUBMIT "Universe = vanilla\n");
print(SUBMIT "Requirements = Memory > 400  && (Arch==\"X86_64\")"); 
#print(SUBMIT "Requirements = Memory > 400  && (Arch==\"INTEL\" || Arch==\"X86_64\")"); 
#print(SUBMIT " && (Machine == \"zebra01.spa.umn.edu\" || Machine == \"zebra02.spa.umn.edu\" || Machine == \"zebra03.spa.umn.edu\" || Machine == \"zebra04.spa.umn.edu\")");
print(SUBMIT "\n");

$i=0;
foreach $infile (@ARGV) {
    $i++;
    $jobCfg=specializeCfg($cfg,$infile);

    $stub=$jobCfg;
    $stub=~s|.*/([^/]+)_cfg.py$|$1|;
    $log="$prodSpace/$jobBase/log/$stub.log";
    $elog="$prodSpace/$jobBase/log/$stub.err";
    print(SUBMIT "Arguments = $rt $prodSpace/$jobBase $jobCfg $log $elog $fname\n");
    print(SUBMIT "Queue\n");
}

close(SUBMIT);


sub specializeCfg($$) {
    my ($inp, $infile)=@_;

    $stub2=$infile;
    $stub2=~s|.*/||g;
#    $stub2=~s|_([0-9]+)[.]root|_$1|;
    $stub2=~s|(.*)[.]root|$1|;
    
    $n=$1;
    if (length($1)<1) {
	$n=$linearn;
	$linearn++;
    }
    $stub3=$cfg;
    $stub3=~s|.*/||g;
    $stub3=~s|_cfg.py||;

    $mycfg="$prodSpace/$jobBase/cfg/".$stub2."_cfg.py";
    print "   $infile --> $stub2 $stub3 ($mycfg) \n";  
    print "$inp $text\n";
    open(INP,$inp);
    open(OUTP,">$mycfg");
    $sector=0;
    while(<INP>) {
	if (/TFileService/) {
	    $sector=7;
	}
	if (/PoolOutputModule/) {
	    $sector=2;
	}
	if (/[.]Source/) {
	    $sector=1;
	}
	if ($sector==2 && /fileName/) {
	    $fname="$prodSpace/$jobBase/".$stub3."_".$n.".root";
	    unlink($fname);
	    print OUTP "       fileName = cms.untracked.string(\"$fname\"),\n";
	} elsif ($sector==7 && /fileName/) {
	    $fname="$prodSpace/$jobBase/".$stub3."_".$n.".root";
	    unlink($fname);
	    print OUTP "       fileName = cms.string(\"$fname\"),\n";
	} elsif ($sector==1 && /fileNames/) {
	    print OUTP "     fileNames=cms.untracked.vstring('file:$infile')\n";
	} else {
	    print OUTP;
	}

	$depth++ if (/\{/ && $sector!=0);
	if (/\}/ && $sector!=0) {
	    $depth--;
	    $sector=0 if ($depth==0);
	}
#	printf("%d %d %s",$sector,$depth,$_);
       
    }
    close(OUTP);
    close(INP);   
    return $mycfg;
}
