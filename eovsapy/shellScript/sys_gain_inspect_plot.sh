#! /bin/bash
# export PYTHONPATH=/home/user/test_svn/python:/common/python/current:/common/python:/common/python/packages/pipeline
source /home/user/.setenv_pyenv38

/home/user/.pyenv/shims/python /common/python/eovsapy-src/eovsapy/sys_gain_inspect.py "$@"
