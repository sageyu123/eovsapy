#! /bin/bash

target_date="$(date -u -d 'yesterday' +%Y%m%d)"
statusfile="/data1/eovsa/fits/synoptic/${target_date:0:4}/${target_date:4:2}/${target_date:6:2}/eovsa.synoptic_pipeline_status.${target_date}.json"
previous_state="$(python3 - <<'PY' "$statusfile"
import json
import sys

statusfile = sys.argv[1]
try:
    with open(statusfile, 'r') as infile:
        print(json.load(infile).get('state', ''))
except Exception:
    print('')
PY
)"

/bin/bash /common/python/eovsapy-src/eovsapy/shellScript/pipeline.sh --clearcache --interp auto --doimport --smart-cal-check > /tmp/pipeline.log 2>&1

current_state="$(python3 - <<'PY' "$statusfile"
import json
import sys

statusfile = sys.argv[1]
try:
    with open(statusfile, 'r') as infile:
        print(json.load(infile).get('state', ''))
except Exception:
    print('')
PY
)"

if [ "$current_state" != "success" ] || [ "$previous_state" = "success" ]; then
  exit 0
fi

/bin/bash /common/python/eovsapy-src/eovsapy/shellScript/pipeline_plt.sh > /tmp/pipeline_plt.log 2>&1
/bin/bash /common/python/eovsapy-src/eovsapy/shellScript/pipeline_compress.sh --ndays 1 > /tmp/pipeline_compress.log 2>&1
