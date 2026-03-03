#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: archive_webplots.sh [--days N] [--dry-run] [--verbose]

Moves web plot files older than N days into date-organized subfolders.

Default behavior (matches pipeline webplot conventions):
  1) /common/webplots/ant_track/track_*YYYYMMDD.jpg
     -> /common/webplots/ant_track/YYYY/

  2) /common/webplots/flaremon/{FLMYYYYMMDD.png,XSPYYYYMMDDHHMMSS.png}
     -> /common/webplots/flaremon/YYYY/

  3) /common/webplots/phasecal/*.{npz,png} (maxdepth 1)
     -> /common/webplots/phasecal/YYYY/YYYYMMDD/

  4) /common/webplots/solpntcal/sys_gain_*20*.jpg (maxdepth 1)
     -> /common/webplots/solpntcal/YYYY/

Options:
  --days N     Age threshold in days (default: 31). Uses find -mtime +N.
  --dry-run    Print planned moves without executing.
  --verbose    Print each file considered/moved.
  --help       Show this help.

Notes:
  - Only files in the top-level of each source directory are considered (maxdepth 1).
  - A date (YYYYMMDD) is extracted from the filename; files without a date are skipped.
  - Override locations with env vars: ANT_TRACK_SRC, ANT_TRACK_DEST, FLAREMON_SRC, FLAREMON_DEST, PHASECAL_ROOT, SOLPNTCAL_ROOT.
EOF
}

DAYS=31
DRY_RUN=false
VERBOSE=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --days)
      if [[ ${2:-} =~ ^[0-9]+$ ]]; then
        DAYS="$2"
        shift 2
      else
        echo "Error: --days requires an integer." >&2
        exit 2
      fi
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --verbose)
      VERBOSE=true
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Error: unknown option: $1" >&2
      echo "Use --help for usage." >&2
      exit 2
      ;;
  esac
done

log() {
  if $VERBOSE; then
    echo "$@"
  fi
}

extract_yyyymmdd() {
  local base="$1"
  if [[ "$base" =~ ([0-9]{8}) ]]; then
    echo "${BASH_REMATCH[1]}"
    return 0
  fi
  return 1
}

ensure_dir() {
  local dir="$1"
  if $DRY_RUN; then
    echo "[Dry Run] mkdir -p \"$dir\""
  else
    mkdir -p "$dir"
  fi
}

move_file() {
  local src="$1"
  local dest_dir="$2"
  ensure_dir "$dest_dir"
  if $DRY_RUN; then
    echo "[Dry Run] mv -n \"$src\" \"$dest_dir/\""
  else
    mv -n "$src" "$dest_dir/"
  fi
}

move_older_than_days() {
  local label="$1"
  local src_dir="$2"
  local dest_root="$3"
  local dest_layout="$4" # "year" or "year_day"
  shift 4
  local -a name_expr=("$@")

  if [[ ! -d "$src_dir" ]]; then
    echo "Skip ($label): missing source dir: $src_dir" >&2
    return 0
  fi

  local local_found=0
  local local_moved=0
  local local_skipped=0

  log "Scan ($label): $src_dir (mtime > $DAYS days)"

  while IFS= read -r -d '' file; do
    local_found=$((local_found + 1))
    local base
    base="$(basename "$file")"

    local yyyymmdd
    if ! yyyymmdd="$(extract_yyyymmdd "$base")"; then
      local_skipped=$((local_skipped + 1))
      log "Skip ($label): no YYYYMMDD in $base"
      continue
    fi

    local year="${yyyymmdd:0:4}"
    local dest_dir
    case "$dest_layout" in
      year)
        dest_dir="${dest_root%/}/$year"
        ;;
      year_day)
        dest_dir="${dest_root%/}/$year/$yyyymmdd"
        ;;
      *)
        echo "Error: unknown dest_layout: $dest_layout" >&2
        exit 2
        ;;
    esac

    log "Move ($label): $file -> $dest_dir/"
    move_file "$file" "$dest_dir"
    local_moved=$((local_moved + 1))
  done < <(
    find "$src_dir" -maxdepth 1 -type f -mtime +"$DAYS" \
      \( "${name_expr[@]}" \) -print0 2>/dev/null || true
  )

  echo "Done ($label): found=$local_found moved=$local_moved skipped=$local_skipped"
}

# Locations/patterns from the request (override via environment variables if needed).
ANT_TRACK_SRC="${ANT_TRACK_SRC:-/common/webplots/ant_track}"
ANT_TRACK_DEST="${ANT_TRACK_DEST:-/common/webplots/ant_track}"

FLAREMON_SRC="${FLAREMON_SRC:-/common/webplots/flaremon}"
FLAREMON_DEST="${FLAREMON_DEST:-/common/webplots/flaremon}"
PHASECAL_ROOT="${PHASECAL_ROOT:-/nas8/eovsa/phasecal}"
SOLPNTCAL_ROOT="${SOLPNTCAL_ROOT:-/common/webplots/solpntcal}"

move_older_than_days \
  "ant_track jpg -> ant_track/YYYY" \
  "$ANT_TRACK_SRC" \
  "$ANT_TRACK_DEST" \
  "year" \
  -name 'track_*.jpg'

move_older_than_days \
  "flaremon png -> flaremon/YYYY" \
  "$FLAREMON_SRC" \
  "$FLAREMON_DEST" \
  "year" \
  -name 'FLM*.png' -o -name 'XSP*.png'

move_older_than_days \
  "phasecal files -> phasecal/YYYY/YYYYMMDD" \
  "$PHASECAL_ROOT" \
  "$PHASECAL_ROOT" \
  "year_day" \
  -name '*.npz' -o -name 'pc*.png'

move_older_than_days \
  "solpntcal jpg -> solpntcal/YYYY" \
  "$SOLPNTCAL_ROOT" \
  "$SOLPNTCAL_ROOT" \
  "year" \
  -name 'sys_gain_*20*.jpg'
