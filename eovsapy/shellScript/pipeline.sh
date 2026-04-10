#! /bin/bash -f

source /home/user/.setenv_pyenv38
# Prefer the system wsclean build; /usr/local/bin/wsclean is linked
# against the older CASA 2 libraries and fails on the current host.
export PATH="/usr/bin:${PATH}"
# /home/user/.pyenv/shims/python /common/python/eovsapy-src/eovsapy/cal_calendar.py `date +\%Y\ \%m`
/home/user/.pyenv/shims/python /common/python/suncasa-src/suncasa/eovsa/eovsa_pipeline.py "$@"
