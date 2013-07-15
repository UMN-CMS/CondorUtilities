#!/bin/sh

PWD=`pwd`
FILE=$1

TFILE="/tmp/${USER}_condor_cmsRun";

echo Executable = ${HOME}/bin/batch_cmsRun > $TFILE
echo Universe = vanilla >> $TFILE
echo Error = ${HOME}/error >> $TFILE
echo Output = ${HOME}/output >> $TFILE
echo -n "Requirements = Memory > 400  && (Arch==\"INTEL\" || Arch==\"X86_64\")" >> $TFILE
echo -n " && (Machine != \"caffeine.spa.umn.edu\")" >> $TFILE
#echo -n " && (Machine == \"zebra01.spa.umn.edu\" || Machine == \"zebra02.spa.umn.edu\" || Machine == \"zebra03.spa.umn.edu\" || Machine == \"zebra04.spa.umn.edu\")" >> $TFILE
#echo -n " && (Machine != \"cms008.spa.umn.edu\" && Machine != \"cms009.spa.umn.edu\")" >> $TFILE
echo  >> $TFILE 

for cfg in $*
do
  FILE=$cfg
  BASE=${FILE##*/}
  LOG="${HOME}/log/${BASE%%_cfg.py}.log"
  ELOG="${HOME}/log/${BASE%%_cfg.py}.elog"

  
  echo Arguments = ${LOCALRT} ${PWD} ${FILE} ${LOG} ${ELOG} >> $TFILE
  echo Queue >> $TFILE


done

condor_submit < $TFILE
rm $TFILE
