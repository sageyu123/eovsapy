# Edit this file to introduce tasks to be run by cron.
#
# Each task to run has to be defined through a single line
# indicating with different fields when the task will be run
# and what command to run for the task
#
# To define the time you can provide concrete values for
# minute (m), hour (h), day of month (dom), month (mon),
# and day of week (dow) or use '*' in these fields (for 'any').#
# Notice that tasks will be started based on the cron's system
# daemon's notion of time and timezones.
#
# Output of the crontab jobs (including errors) is sent through
# email to the user the crontab file belongs to (unless redirected).
#
# For example, you can run a backup of all your user accounts
# at 5 a.m every week with:
# 0 5 * * 1 tar -zcf /var/backups/home.tgz /home/
#
# For more information see the manual pages of crontab(5) and cron(8)
#
# m h  dom mon dow   command
# Keep a list of 10 latest IDB files, for use on status web page
* * * * * ls /data1/IDB | tail > /common/webplots/IDBlist.txt 2>&1

# Cronjob to test packet accumulations
# * * * * * /bin/rm /home/user/DPP_PACKET_TEST.txt; /home/user/test_svn/Miriad/packet_dump_test/dpp_packet_test > /dev/null 2>&1
# cronjob for scanheaders
## * * * * * /bin/rm /home/user/DPP_SCANHEADER_TEST.txt; /home/user/test_svn/Miriad/packet_dump_test/dpp_scanheader_test > /dev/null 2>&1

* * * * * cd /home/user/test_svn/Miriad/dpp50B; /bin/csh /home/user/test_svn/Miriad/dpp50B/makerun_dppxmp.sh > /dev/null 2>&1
* * * * * sleep 10;cd /home/user/test_svn/Miriad/dpp50B; /bin/csh /home/user/test_svn/Miriad/dpp50B/makerun_dppxmp.sh > /dev/null 2>&1
* * * * * sleep 20;cd /home/user/test_svn/Miriad/dpp50B; /bin/csh /home/user/test_svn/Miriad/dpp50B/makerun_dppxmp.sh > /dev/null 2>&1
* * * * * sleep 30;cd /home/user/test_svn/Miriad/dpp50B; /bin/csh /home/user/test_svn/Miriad/dpp50B/makerun_dppxmp.sh > /dev/null 2>&1
* * * * * sleep 40;cd /home/user/test_svn/Miriad/dpp50B; /bin/csh /home/user/test_svn/Miriad/dpp50B/makerun_dppxmp.sh > /dev/null 2>&1
* * * * * sleep 50;cd /home/user/test_svn/Miriad/dpp50B; /bin/csh /home/user/test_svn/Miriad/dpp50B/makerun_dppxmp.sh > /dev/null 2>&1

# cronjob to move the status files
# 0,10,20,30,40,50 * * * * /bin/csh /home/user/test_svn/shell_scripts/dpp_proc_status_move.csh > /dev/null 2>&1

# cronjob to create TPCAL calibration files in SQL database
40 18,21 * * * touch /data1/TPCAL/LOG/TPC$(date +\%Y\%m\%d).log; /bin/bash /home/user/test_svn/shell_scripts/tpcal.sh >> /data1/TPCAL/LOG/TPC$(date +\%Y\%m\%d).log 2>&1

# cronjob to create the Flare Monitor plots for the web page in /common/webplots/flaremon (every 10 min from 13:00-02:00 UT)
2,12,22,32,42,52 0,1,2,13,14,15,16,17,18,19,20,21,22,23 * * * touch /data1/TPCAL/LOG/FLM$(date +\%Y\%m\%d).log; /bin/bash /home/user/test_svn/shell_scripts/flare_monitor.sh >> /data1/TPCAL/LOG/FLM$(date +\%Y\%m\%d).log 2>&1

# cronjob to do the final Flare Monitor plot for the day (03:00 UT)
# 0 3 * * * touch /data1/TPCAL/LOG/FLM$(date +\%Y\%m\%d).log; /bin/bash /home/user/test_svn/shell_scripts/flare_monitor.sh >> /data1/TPCAL/LOG/FLM$(date +\%Y\%m\%d).log 2>&1

# cronjob to append RT median data to daily file (every 1 min from 13:00-02:00 UT)
* 0,1,2,13,14,15,16,17,18,19,20,21,22,23 * * * touch /data1/RT/RT.log; /bin/bash /home/user/test_svn/shell_scripts/RT_monitor.sh >> /data1/RT/RT.log 2>&1

# cronjob to run adc_plot.py to monitor and update the ADC levels.  Starts at 11:00 UT and goes for 16 hours (until 3:00 UT).
@reboot /bin/bash /home/user/test_svn/shell_scripts/adc_plot.sh > /dev/null 2>&1
0 11 * * * /bin/bash /home/user/test_svn/shell_scripts/adc_plot.sh > /dev/null 2>&1

# cronjob to create the phasecal plots for the web page in /common/webplots/phasecal (every 5 min)
0,5,10,15,20,25,30,35,40,45,50,55 * * * * /bin/bash /home/user/test_svn/shell_scripts/pcal_anal.sh >> /home/user/ychai/routine.log 2>&1

# cronjob to remove IDB files that are more than 21 days old, note that IDBs are copied
10 3 * * * find /data1/IDB -name \* -mtime +21 -exec /bin/rm -rf {} \; 1>/dev/null 2>&1

# cronjob to remove flaretest files that are more than 21 days old, note that flaretests are copied
10 4 * * * find /data1/RT -name "*flaretest*" -mtime +21 -exec /bin/rm -rf {} \; 1>/dev/null 2>&1

#cronjob to start the dpp_fix_packets.py program
@reboot /bin/bash /home/user/test_svn/shell_scripts/start_fix_packets.sh

#crontab to check to ensure dpp_fix_packets.py is still running
*/10 * * * * /home/user/test_svn/shell_scripts/start_fix_packets.sh

#crontab to check if data recording has stopped and to run rmlock if it has. It currently does not remove the lock file as it is in a test mode.
@reboot /bin/bash /home/user/owen/check_datarec.sh > /tmp/datarec.log