#!/usr/bin/perl

use Getopt::Long;

#------------------------
#$prodSpace=$ENV{"HOME"}."/work";
$prodSpace="/local/cms/user/".$ENV{"USER"};
$batch=10;
$startPoint=0;
$nosubmit='';


$executable=$ENV{"HOME"}."/bin/batch_cmsRun";
$rt=$ENV{"LOCALRT"};
$arch=$ENV{"SCRAM_ARCH"};

$jobBase="default";

GetOptions("batch=i" => \$batch,
	   "start=i" => \$startPoint,
	   "nosubmit" => \$nosubmit,
	   "prodspace=s" => \$prodSpace,
	   "jobname=s" => \$jobBase);

if ($#ARGV<1) {
    print "Usage: [BASE CONFIG] [NAME OF FILE CONTAINING LIST OF FILENAMES] \n\n";
    print "    --batch (number of files per jobs) (default $batch)\n";
    print "    --start (output file number for first job) (default $startPoint)\n";
    print "    --jobname (name of the job) (default based on base config)\n";
    print "    --prodSpace (production space) (default $prodSpace)\n";
    print "    --nosubmit (don't actually submit, just make files)\n";
    exit(1);
}

$basecfg=shift @ARGV;
$filelist=shift @ARGV;

if ($jobBase eq "default") {
    my $stub3=$basecfg;
    $stub3=~s|.*/||g;
    $stub3=~s|_cfg.py||;
    $stub3=~s|[.]py||;
    $jobBase=$stub3;
}


if (length($rt)<2) {
	print "You must run \"cmsenv\" in the right release area\n";
        print "before running this script!\n";
	exit(1);
}

#------------------------

print "Setting up a job based on $basecfg into $jobBase using $filelist\n";
if ($nosubmit) {
    print "  Will not actually submit this job\n";
}

$cfg=$basecfg;

system("mkdir -p $prodSpace/$jobBase");
system("mkdir -p $prodSpace/logs");
mkdir("$prodSpace/$jobBase/cfg");
mkdir("$prodSpace/$jobBase/log");

$linearn=0;

srand(); # make sure rand is ready to go
if ($nosubmit) {
    open(SUBMIT,">condor_submit.txt");
} else {
    open(SUBMIT,"|condor_submit");
}
print(SUBMIT "Executable = $executable\n");
print(SUBMIT "Universe = vanilla\n");
#print(SUBMIT "Requirements = Memory > 400 && (Arch==\"INTEL\" || Arch==\"X86_64\")");
print(SUBMIT "Output = $prodSpace/logs/output\n");
print(SUBMIT "Error = $prodSpace/logs/error\n");
#print(SUBMIT "Requirements = Memory > 400 && (Arch==\"X86_64\")");
print(SUBMIT "Request_memory = 400");
print(SUBMIT "Requirements = Arch==\"X86_64\"");
print(SUBMIT " && (Machine != \"zebra01.spa.umn.edu\" && Machine != \"zebra02.spa.umn.edu\" && Machine != \"zebra03.spa.umn.edu\" && Machine != \"caffeine.spa.umn.edu\")");
print(SUBMIT " && (Machine != \"gc1-ce.spa.umn.edu\" && Machine != \"gc1-hn.spa.umn.edu\" && Machine != \"gc1-se.spa.umn.edu\" && Machine != \"red.spa.umn.edu\")");
#print(SUBMIT " && (Machine != \"zebra04.spa.umn.edu\")");
print(SUBMIT "\n");
print(SUBMIT "+CondorGroup=\"cmsfarm\"\n");

open(FLIST,$filelist);
while (<FLIST>) {
    chomp;
    push @flist,$_;
}
close(FLIST);

$i=0;
$ii=$startPoint-1;

while ($i<=$#flist) {
    $ii++;

    @jobf=();    
    for ($j=0; $j<$batch && $i<=$#flist; $j++) {
	push @jobf,$flist[$i];
	$i++;
    }

    $jobCfg=specializeCfg($cfg,$ii,@jobf);

    $stub=$jobCfg;
    $stub=~s|.*/([^/]+)_cfg.py$|$1|;
    $log="$prodSpace/$jobBase/log/$stub.log";
    $elog="$prodSpace/$jobBase/log/$stub.err";
    $sleep=($ii*2);
    print(SUBMIT "Arguments = $arch $rt $prodSpace/$jobBase $jobCfg $log $elog $fname $sleep $jobf[0] \n");
    print(SUBMIT "Queue\n");
}

close(SUBMIT);


sub specializeCfg($$@) {
    my ($inp, $index, @files)=@_;


    $stub2=$jobBase;
    $stub2.=sprintf("_%03d",$index);

    $mycfg="$prodSpace/$jobBase/cfg/".$stub2."_cfg.py";
    print "   $inp $index --> $stub2 ($mycfg) \n";  
    #print "$inp $text\n";
    open(INP,$inp);
    open(OUTP,">$mycfg");
    $sector=0;
    $had2=0;
    $had3=0;
    while(<INP>) {
	if (/TFileService/) {
	    $sector=2;
	    $had2=1;
	}
	if (/PoolOutputModule/) {
	    $sector=3;
	    $had3=1;
	}
	if (/[.]Source/) {
	    $sector=1;
	}
	if ($sector==2 && /^[^\#]*fileName/) {
	    if ($had3==1) {
		$fname="$prodSpace/$jobBase/".$stub2."-hist.root";
	    } else {
		$fname="$prodSpace/$jobBase/".$stub2.".root";
	    }
	    unlink($fname);
	    print OUTP "       fileName = cms.string(\"$fname\"),\n";
	} elsif ($sector==3 && /^[^\#]*fileName/) {
	    if ($had2==1) {
		$fname="$prodSpace/$jobBase/".$stub2."-pool.root";
	    } else {
		$fname="$prodSpace/$jobBase/".$stub2.".root";
	    }
	    unlink($fname);
	    print OUTP "       fileName = cms.untracked.string(\"$fname\"),\n";
	} elsif ($sector==1 && /^[^\#]*fileNames/) {	    
	    print OUTP "    fileNames=cms.untracked.vstring(\n";
	    for ($qq=0; $qq<=$#files; $qq++) {
		$storefile=$files[$qq];
		if ($storefile=~/store/) {
			$storefile=~s|.*/store|/store|;	
		} else {
			$storefile="file:".$storefile;
		}
			
		print OUTP "         '".$storefile."'";
		print OUTP "," if ($qq!=$#files);
		print OUTP "\n";
	    }		
	    print OUTP "     )\n";
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
