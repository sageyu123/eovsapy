#! /bin/bash

/bin/bash /common/python/eovsapy-src/eovsapy/shellScript/pipeline.sh --clearcache --interp auto > /tmp/pipeline.log 2>&1
/bin/bash /common/python/eovsapy-src/eovsapy/shellScript/pipeline_plt.sh > /tmp/pipeline_plt.log 2>&1
/bin/bash /common/python/eovsapy-src/eovsapy/shellScript/pipeline_compress.sh --ndays 1 > /tmp/pipeline_compress.log 2>&1