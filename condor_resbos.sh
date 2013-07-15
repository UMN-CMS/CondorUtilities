#!/bin/sh

workarea=/data/whybee0c/user/jmmans/resbos

for jobfile in $*
do

jobname=${jobfile%%.*}
jobname=${jobname##*/}

echo $jobname $jobfile

scriptname=${workarea}/script/${jobname}.sh
log=${workarea}/log/${jobname}.log

line1=`head -n1 ${jobfile}`
line2=`head -n2 ${jobfile} | tail -1`
line3=`head -n3 ${jobfile} | tail -1`

echo $line1 
echo $line2
echo $line3

grid1=${line2%% *>*}
grid2=${line3%% *>*}

rest=`wc -l ${jobfile}`
rest=${rest%% *}
rest=$((rest - 3))

cat > ${scriptname} <<EOF
#!/bin/sh
mkdir /export/scratch/tmp/resbos-${jobname}
cd /export/scratch/tmp/resbos-${jobname}
ln -s ${grid1} main.out
ln -s ${grid2} y.out

cat > resbos.in <<EOQ
${line1}
main.out
y.out
EOF

tail -n $rest $jobfile >> ${scriptname}

cat >> ${scriptname} <<EOF
EOQ
hostname > ${log}
export LD_LIBRARY_PATH=/local/cms/other/resbos/root/lib
ulimit -f unlimited
/local/cms/other/resbos/resbos >> ${log} 2>&1
mv resbos.root ${workarea}/$jobname.root
gzip -f ${log}
cd /export/scratch/tmp
rm -rf /export/scratch/tmp/resbos-${jobname}
EOF

chmod +x ${scriptname}

# now start condor
condor_submit <<EOF
Executable = ${scriptname}
Universe = vanilla 
Error = ${HOME}/error
Output = ${HOME}/output
Queue
EOF
done
