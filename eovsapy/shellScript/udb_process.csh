#! /bin/csh

#Processes IDB files into UDB files, also creates FITS format files
#Removed ssw_batch, setup_ssw causes segmentation faults, jmm, 2018-10-24
#So there are no images in the /fits/images directories
# Original script: /home/user/test_svn/shell_scripts/udb_process.csh

#set up python path variable
setenv PYTHONPATH '/home/user/test_svn/python:/common/python/current'
#If this is set, Python won’t add the user site directory to sys.path
setenv PYTHONNOUSERSITE 1

#For Miriad
source ~/miriad/miriad_start.csh

#Need an IDL_DIR variable
#setenv IDL_DIR /usr/local/exelis/idl82
# make sure IDL_PATH is intialized
#if !( $?IDL_PATH ) then
#   setenv IDL_PATH '<IDL_DEFAULT>'
#endif

#Define SSW, SSWDB
#setenv SSW /ssw
#setenv SSWDB /sswdb

# SSW setup
#source $SSW/gen/setup/setup.ssw
#To get different Instrument setups
#setssw ovsa eovsa

# Add local directories to IDL_PATH
#setenv IDL_STARTUP /home/user/cronjob_idl_startup.pro

cd /home/user/workdir
setenv datestr `date +%Y%m%d`
#if ! (-d /data1/eovsa/fits/images/${datestr} ) then
#   mkdir /data1/eovsa/fits/images/${datestr}
#endif
set line="$datestr"
/usr/bin/python /home/user/test_svn/python/udb_process.py >> /data1/processing/LOG/udb_process_log.txt_$line
#ssw_batch /home/user/workdir/udb_process_spec.pro /home/user/workdir/udb_process_spec.out