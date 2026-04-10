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
# Run the UDB process script every 5 minutes all day
# 0,5,10,15,20,25,30,35,40,45,50,55 * * * * cd /data1/workdir; /bin/csh /home/user/test_svn/shell_scripts/udb_process.csh > /dev/null 2>&1
*/5 * * * * /usr/bin/flock -n /data1/processing/udb_process.flock -c "/bin/csh /home/user/test_svn/shell_scripts/udb_process.csh"
# Add directories for next years UDB, IDB, IFDB, etc
0 12 25 12 * cd /data1/workdir; /bin/csh /home/user/test_svn/shell_scripts/add_yrdir.csh > /dev/null 2>&1
# This job is run from DPP, not Pipeline, so commented out here
#   Run the process that analyzes total power calibrations every 6 minutes (does nothing if TPCAL is not recent)
#   1,6,11,16,21,26,31,36,41,46,51,56 17,18,19,20,21,22,23 * * * touch /data1/TPCAL/LOG/TPC$(date +\%Y\%m\%d).log; /usr/bin/python /common/python/current/calibration.py >> /data1/TPCAL/LOG/TPC$(date +\%Y\%m\%d).log 2>&1

# Run the process that creates all-day full-resolution total-power and cross-power FITS files
0 7 * * * cd /data1/workdir; /bin/tcsh /home/user/test_svn/shell_scripts/pipeline_allday_fits.csh --clearcache > /tmp/pipeline_fits.log 2>&1

# Run the process that creates the daily annotated 1-min resolution cross-power spectrum
0 9 * * * cd /data1/workdir; /bin/csh /home/user/test_svn/shell_scripts/daily_xsp.csh > /tmp/daily_xsp.log 2>&1

# Run the process that creates a summary of the antenna tracking status every 5 minutes
# Use flock to prevent multiple instances from running simultaneously
# Run every 5 minutes between 12:00 and 23:59 UT
*/5 12-23 * * * /usr/bin/flock -n /tmp/cron_fig_ant_track.lock -c "bash /common/python/eovsapy-src/eovsapy/shellScript/daily_track_plot.sh" >> /tmp/daily_track_plot.log 2>&1
# Run every 5 minutes between 00:00 and 02:59 UT
*/5 0-2 * * * /usr/bin/flock -n /tmp/cron_fig_ant_track.lock -c "bash /common/python/eovsapy-src/eovsapy/shellScript/daily_track_plot.sh" >> /tmp/daily_track_plot.log 2>&1

# Run the SOLPNTCAL system-gain inspection plots after the expected 18:30 and 21:30 UT scans
# (about 20 minutes later, at 18:50 and 21:50 UT)
50 18,21 * * * /usr/bin/flock -n /tmp/cron_fig_sys_gain.lock -c "bash /common/python/eovsapy-src/eovsapy/shellScript/sys_gain_inspect_plot.sh" >> /tmp/sys_gain_inspect_plot.log 2>&1

# Poll hourly for observer-written calibration readiness, then run the full-disk image pipeline once the day is ready
5 * * * * EOVSA_PIPELINE_CRON=1 /usr/bin/flock -n /tmp/pipeline_fdimg.lock -c "cd /data1/workdir; /bin/bash /common/python/eovsapy-src/eovsapy/shellScript/pipeline_fdimg.sh" > /tmp/pipeline_fdimg.log 2>&1

# Run OVSA spectrogram script every day
0 */6 * * * /bin/bash -c "cd /data1/workdir; source /home/user/.setenv_pyenv38; /home/user/.pyenv/shims/python /common/python/suncasa-src/suncasa/utils/ovsa_spectrogram.py" > /tmp/ovsa_spectrogram.log 2>&1

# Run the process that creates the raw UDBms files
# 0,30 * * * * touch /data1/eovsa/fits/UDBms/LOG/UDB2MS$(date +%Y%m%d).log;/bin/tcsh /home/user/sjyu/udb2ms.csh >> /data1/eovsa/fits/UDBms/LOG/UDB2MS$(date +%Y%m%d).log 2>&1

# Capture webcam screenshot once/minute
* * * * * cd /common/webplots/flaremon; wget -O snap.jpg "http://192.168.24.178:88/cgi-bin/CGIProxy.fcgi?cmd=snapPicture2&usr=guest&pwd=snap4me"

# Create GOES SXR plots once/minute
* * * * * cd /common/webplots/flaremon; /usr/bin/python /common/python/current/goes.py

# Get the most recent RSTN noon flux values and write them to SQL
0 3 * * * cd /data1/workdir; /bin/tcsh /home/user/test_svn/shell_scripts/noaa2sql.csh >> /tmp/rstn.log 2>&1

# Run the script that copies flaretest_*.txt files from /dppdata1/RT to /data1/eovsa/fits/FTST, starts at 1300,
#should quit at 0300 the following day
0 13 * * * cd /data1/workdir; /home/user/jimm/python3/flaretest_move_script.py > /tmp/flaretest_move_script.log 2>&1

# Archive webplots older than 7 days every day at 6 UT
0 6 * * * /bin/bash /common/python/eovsapy-src/eovsapy/shellScript/archive_webplots.sh --days 7 > /tmp/archive_webplots.log 2>&1

## OVRO-LWA scripts go below this line:

#* * * * * cd /nas5/ovro-lwa-data; touch LOG/beamcopy_$(date +\%Y\%m\%d).log; beam/software/auto_beamcopy.sh >> LOG/beamcopy_$(date +\%Y\%m\%d).log 2>&1
# Sync LWA quicklook plots at 1-min cadence for the day
#* * * * * touch /nas6/ovro-lwa-data/LOG/plotscopy_$(date +\%Y\%m\%d).log; source /home/user/.setenv_pyenv38; /home/user/.pyenv/shims/python /home/user/bchen/daily_lwa_file_transfer.py --plots --ndays 2 >> /nas6/ovro-lwa-data/LOG/plotscopy_$(date +\%Y\%m\%d).log 2>&1
* * * * * touch /nas6/ovro-lwa-data/LOG/plotscopy_$(date +\%Y\%m\%d).log; source /home/user/.setenv_pyenv38; /home/user/.pyenv/shims/python /home/user/lwa-op/daily_lwa_file_transfer.py --plots --ndays 2 >> /nas6/ovro-lwa-data/LOG/plotscopy_$(date +\%Y\%m\%d).log 2>&1
# Sync LWA quicklook plots every day at 3 UT for the past 7 days
0 3 * * * touch /nas6/ovro-lwa-data/LOG/hdfcopy_$(date +\%Y\%m\%d).log; source /home/user/.setenv_pyenv38; /home/user/.pyenv/shims/python /home/user/lwa-op/daily_lwa_file_transfer.py --plots --ndays 7 >> /nas6/ovro-lwa-data/LOG/hdfcopy_$(date +\%Y\%m\%d).log 2>&1
# Sync LWA hdf files every day at 3 UT for the past 7 days
0 3 * * * touch /nas6/ovro-lwa-data/LOG/hdfcopy_$(date +\%Y\%m\%d).log; source /home/user/.setenv_pyenv38; /home/user/.pyenv/shims/python /home/user/lwa-op/daily_lwa_file_transfer.py --hdf --ndays 7 >> /nas6/ovro-lwa-data/LOG/hdfcopy_$(date +\%Y\%m\%d).log 2>&1
# Sync LWA beamforming data every day at 3 UT for the past 7 days
0 3 * * * touch /nas6/ovro-lwa-data/LOG/beamcopy_$(date +\%Y\%m\%d).log; source /home/user/.setenv_pyenv38; /home/user/.pyenv/shims/python /home/user/lwa-op/daily_lwa_file_transfer.py --beam --ndays 7 >> /nas6/ovro-lwa-data/LOG/beamcopy_$(date +\%Y\%m\%d).log 2>&1
# Sync LWA calibration tables every day at 3 UT
0 3 * * * bash /home/user/bchen/lwa_sync_cal.sh
# Sync spectrum from tmp to database dir
0 4 * * * rsync -av  /sbdata/lwa-spec-tmp/spec_lv1/ /nas7a/beam/fits_v1/ >> /nas6/ovro-lwa-data/LOG/beamfits_copy_$(date +\%Y\%m\%d).log 2>&1
# Make LWA slow image file number plot every 10 minutes
2,12,22,32,42,52 * * * * rsync -av solarpipe@lwacalim07:/opt/devel/solarpipe/operation/lwa_hourly_image_counts_db.fch.csv /home/user/lwa-op/
5,15,25,35,45,55 * * * * cd /home/user/lwa-op/; source /home/user/.setenv_pyenv38; /home/user/.pyenv/shims/python /home/user/lwa-op/lwa_count_daily_images.py --doplot
# * * * * * source /home/user/.setenv_pyenv38; /home/user/.pyenv/shims/python /home/user/lwa-op/lwa_count_daily_images.py --doplot >> /home/user/lwa-op/LOG/lwa_number_plot_$(date +\%Y\%m\%d).log 2>&1
# Update LWA slow viz hdf level 1 daily image file number counts every day
0 10 * * * cd /home/user/lwa-op/; source /home/user/.setenv_pyenv38; /home/user/.pyenv/shims/python /home/user/lwa-op/lwa_count_daily_images.py --scandays --ndays 3

## run flare detection routine (find_flare4date.py) after each reboot
@reboot /common/python/current/start_flare_detect.sh
# health check every 5 minutes: restart if missing or likely hung
*/5 * * * * /bin/bash /common/python/eovsapy-src/eovsapy/shellScript/flare_detect_watchdog.sh

## Influx streamer watchdog retired on 2026-03-23.
# The supported Influx ingestion path is acc_exporter -> Telegraf -> InfluxDB.
#* * * * * export EOVSA_PYTHON_ENV_FILE=/home/user/.eovsa_exporter_env; /bin/bash /common/python/eovsapy-src/eovsapy/shellScript/influx_stream_watchdog.sh
