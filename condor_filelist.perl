#!/usr/bin/perl

#------------------------
#$prodSpace=$ENV{"HOME"}."/work";
#$prodSpace="/data/whybee0c/user/haupt/Electrons/TNPTREE10";
$prodSpace="/local/cms/user/gude/zshape/";
$executable=$ENV{"HOME"}."/bin/batch_cmsRun";
$rt=$ENV{"LOCALRT"};

if ($#ARGV<2) {
    print "Usage: [PREFIX] [BASE CONFIG] [NAME OF FILE CONTAINING LIST OF FILENAMES] [#
files/job] [Number of events per job]\n\n";
    exit(1);
}

$startPoint=0;
$numbevents=-1;

$postfix=shift@ARGV;
$basecfg=shift @ARGV;
$filelist=shift @ARGV;
$batch=shift @ARGV;
$numbevents=shift @ARGV if ($#ARGV>=0);
$startPoint=shift @ARGV if ($#ARGV>=0);

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

#print(SUBMIT "Requirements = Memory > 400 && (Arch==\"INTEL\" || Arch==\"X86_64\")");
#print(SUBMIT "Requirements = Memory > 100 && (Arch==\"X86_64\")\n");
print(SUBMIT "Requirements = Memory > 100 && (Arch==\"X86_64\")");
print(SUBMIT "&& (SlotID == 3 || SlotID == 4)\n");
print(SUBMIT "notify_user = alex.public.account+CMSCondor\@gmail.com\n");
print(SUBMIT "+CondorGroup = \"cmsfarm\" \n");

#print(SUBMIT " && (Machine == \"zebra01.spa.umn.edu\" || Machine ==
#\"zebra02.spa.umn.edu\" || Machine == \"zebra03.spa.umn.edu\" || Machine ==
#\"zebra04.spa.umn.edu\")");
#print(SUBMIT "\n");
#print(SUBMIT " && (Machine != \"cms008.spa.umn.edu\" && Machine != \"cms009.spa.umn.edu\" ) ");
print(SUBMIT " Rank = (Machine != \"scorpion5.spa.umn.edu\")");
print(SUBMIT "\n");

open(FLIST,$filelist);
while (<FLIST>) {
    chomp;
    push @flist,$_;
}
close(FLIST);

$i=0;
$ii=$startPoint;
while ($i<=$#flist) {
    $ii++;

    @jobf=();
    for ($j=0; $j<$batch && $i<=$#flist; $j++) {
        push @jobf,$flist[$i];
        $i++;
    }

    $jobCfg=specializeCfg($cfg,$ii,@jobf);

    $sleeptime=$ii*2;
    $stub=$jobCfg;
    $stub=~s|.*/([^/]+)_cfg.py$|$1|;
    $log="$prodSpace/$jobBase/log/$stub.log";
    $elog="$prodSpace/$jobBase/log/$stub.err";
    print(SUBMIT "Arguments = $rt $prodSpace/$jobBase $jobCfg $log $elog $fname $sleeptime\n");
    print(SUBMIT "Queue\n");
}

close(SUBMIT);


sub specializeCfg($$@) {
    my ($inp, $index, @files)=@_;


    $stub2=$inp;
    $stub2=~s|^.*/||;
    $stub2=~s|_cfg[.]py$||;
    $stub2=~s|[.]py$||;
    $stub2.=sprintf("-%s_%03d",$postfix,$index);

    $mycfg="$prodSpace/$jobBase/cfg/".$stub2."_cfg.py";
    print "   $inp $index --> $stub2 ($mycfg) \n";  
    print "$inp $text\n";
    open(INP,$inp);
    open(OUTP,">$mycfg");
    $sector=0;
    while(<INP>) {
        if (/PoolOutputModule/) {
            $sector=2;
        }
	if (/TFileService/) {
            $sector=3;
        }
        if (/[.]Source/) {
            $sector=1;
        }
	if ($sector==0 && /maxEvents/) {
	    $sector=4;
	    $depth=0;
	}
	if (/ISpyService/) {
            $sector=5;
	}
        if ($sector==2 && /fileName/) {
            $fname="$prodSpace/$jobBase/".$stub2.".root";
            unlink($fname);
            print OUTP "       fileName = cms.untracked.string(\"$fname\"),\n";
        } elsif ($sector==3 && /fileName/) {
            $fname="$prodSpace/$jobBase/TF".$stub2.".root";
            unlink($fname);
            print OUTP "       fileName = cms.string(\"$fname\"),\n";
        } elsif ($sector==4 && /input/) {
	    /(\s*)untracked\s+uint32\s+([A-Za-z0-9]+)\s*=/;
	   # $seed=20000000+$i*10000+int(rand(10000)); 
	   # print OUTP $1."untracked uint32 ".$2." = $seed\n";
           print OUTP "\t input = cms.untracked.int32($numbevents)\n";
        } elsif ($sector==1 && /fileNames/) {            
            print OUTP "    fileNames=cms.untracked.vstring(\n";
            for ($qq=0; $qq<=$#files; $qq++) {
                $storefile=$files[$qq];
                #uncommented to use logical storate name /store/... instead of physical file system name
                #$storefile=~s|.*/store|/store|;                
                print OUTP "         'file:".$storefile."'";
                print OUTP "," if ($qq!=$#files);
                print OUTP "\n";
            }                
            print OUTP "     )\n";
        } elsif ($sector==5 && /outputFileName/) {
	    $fname="$prodSpace/$jobBase/".$stub2.".ig";
	    print OUTP "    outputFileName=cms.untracked.string('".$fname."'),"
	} elsif ($sector==0 && /process.tpa0.FitFileName/) {
	    $outfname="$prodSpace/$jobBase/SCtoGSFEff.$stub2".".root";
            unlink($outfname);
            print OUTP "process.tpa0.FitFileName = \'$outfname\'\n";
        } elsif ($sector==0 && /process.tpa1.FitFileName/) {
	    $outfname="$prodSpace/$jobBase/GSFtoIsoEff.$stub2".".root";
            unlink($outfname);
            print OUTP "process.tpa1.FitFileName = \'$outfname\'\n";
        } elsif ($sector==0 && /process.tpa2.FitFileName/) {
	    $outfname="$prodSpace/$jobBase/IsotoIDEff.$stub2".".root";
            unlink($outfname);
            print OUTP "process.tpa2.FitFileName = \'$outfname\'\n";
        } elsif ($sector==0 && /process.tpa3.FitFileName/) {
	    $outfname="$prodSpace/$jobBase/IDtoHLTEff.$stub2".".root";
            unlink($outfname);
            print OUTP "process.tpa3.FitFileName = \'$outfname\'\n";
        } elsif ($sector==0 && /process.tpa4.FitFileName/) {
	    $outfname="$prodSpace/$jobBase/HFEIDEff.$stub2".".root";
            unlink($outfname);
            print OUTP "process.tpa4.FitFileName = \'$outfname\'\n";
        } elsif ($sector==0 && /process.tpa5.FitFileName/) {
	    $outfname="$prodSpace/$jobBase/MCtoSCEff.$stub2".".root";
            unlink($outfname);
            print OUTP "process.tpa5.FitFileName = \'$outfname\'\n";

	} else {
            print OUTP;
        }

        $depth++ if (/\{/ && $sector!=0);
        if (/\}/ && $sector!=0) {
            $depth--;
            $sector=0 if ($depth==0);
        }
#        printf("%d %d %s",$sector,$depth,$_);
       
    }
    close(OUTP);
    close(INP);   
    return $mycfg;
}
