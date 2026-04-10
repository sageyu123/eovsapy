# EOVSAPY

![Latest Version](https://img.shields.io/pypi/v/eovsapy.svg)

EOVSAPY is a Python library dedicated to the processing and analysis of data from the Expanded Owens Valley Solar Array. For more details about the project, visit our [homepage](https://github.com/ovro-eovsa/eovsapy).

## Installation

### Prerequisites

Before installing EOVSAPY, ensure you have `pip` installed. For instructions, refer to the [pip installation guide](https://packaging.python.org/tutorials/installing-packages/).

### Installing EOVSAPY

EOVSAPY can be easily installed using pip. Run the following command:

```bash
pip install eovsapy
```

### Configuring Access to the Interim Database (IDB)

To process and calibrate EOVSA raw "Interim" Database (IDB) data, access to the SQL database containing the calibration data is required. Perform the following steps to configure access:

1. **Obtain Database Credentials**:
Contact sijie.yu@njit.edu to request the `<username>`, `<account_name>`, and `<password>` for database access.

2. **Create a `.netrc` File**:

   Create a `.netrc` file in your home directory (`$HOME`) with the following contents, replacing `<username>`, `<account_name>`, and `<password>` with the actual database credentials:

   ```bash
   machine eovsa-db0.cgb0fabhwkos.us-west-2.rds.amazonaws.com
           login <username>
           account <account_name>
           password <password>
   ```

3. **Secure the `.netrc` File**:

   To ensure that the file is only accessible by you, set its permissions to only allow owner read/write:

   ```bash
   chmod 0600 ~/.netrc
   ```

## Operational Telemetry Helper

`eovsapy.telemetry` provides a narrow Python 3 helper for replaying saved
stateframe logs into normalized operational telemetry records. This is intended
for sidecar/dashboard export paths such as a future InfluxDB writer. It is not
an authoritative replacement for the existing historian, SQL tables, or legacy
control logging.

Current scope:

- source of truth for binary layout: versioned stateframe XML companion file
- supported first step: replay/tail of saved `sf_*.log` files
- optional live mode: direct ACC stateframe socket reads when `acc.ini` and the
  matching stateframe XML are available
- preserved on every record: embedded stateframe version, XML definition path,
  source path, and frame index
- intentionally omitted: calibration/product metadata and any attempt to flatten
  the full stateframe

Example:

```python
from eovsapy.telemetry import iter_operational_telemetry

for record in iter_operational_telemetry(
    "/path/to/sf_20260320_v66.0.log",
    xml_path="/path/to/stateframe_v66.00.xml",
):
    print(record["timestamp"]["iso_utc"], record["weather"]["temperature"])
```

Live example:

```python
from eovsapy.telemetry import iter_live_operational_telemetry

for record in iter_live_operational_telemetry(
    "/path/to/acc.ini",
    xml_path="/path/to/stateframe_v66.00.xml",
    host="acc.solar.pvt",
    poll_interval=2.0,
):
    print(record["timestamp"]["iso_utc"], record["antennas"][0]["track_flag"])
```

The normalized record is structured for a later writer layer:

- `source`: replay provenance
- `schema`: embedded stateframe version and definition version
- `schedule`: task/scan-state/run-mode summary
- `weather`
- `lo`
- `fem_banks`
- `power`
- `antennas`: per-antenna az/el, track state, frontend, and DCM subset

The normalized records are intended to be consumed by a separate InfluxDB
writer, so Grafana can query InfluxDB directly without knowing the legacy
binary stateframe layout.

## Historian Replacement Work

`eovsapy.historian` is the raw-preserving replacement-oriented layer for the
legacy `fBin` / `hBin` SQL write path. It is designed for replay, backfill, and
side-by-side equivalence testing before any cutover.

Key design point:

- InfluxDB alone is not a good first full-fidelity replacement target if the
  design requires flattening all nested stateframe/scanheader content into
  ordinary time-series measurements.
- The recommended first model is hybrid:
  - raw binary frame preservation in a raw store
  - queryable envelope/index metadata in an index store
  - optional parsed operational projections later for Grafana

Implemented hybrid building blocks:

- `FileSystemRawFrameStore`: raw payload persistence by SHA-256
- `JsonlEnvelopeIndexStore`: simple append-only audit/index store
- `InfluxDBEnvelopeStore`: Influx-style envelope/index scaffold
- `TimescaleDBEnvelopeScaffold`: Timescale/Postgres-style envelope row scaffold
- `PostgreSQLPayloadStore`: PostgreSQL `BYTEA` payload store keyed by SHA-256
- `TimescaleDBEnvelopeStore`: real Timescale hypertable/index implementation
- `HybridHistorianSink`: composite sink that writes raw payloads and envelopes together

Concrete `hBin` replacement path:

- `scanheader_to_timescaledb.py`: live or replay ingest for raw scanheader
  records into PostgreSQL/TimescaleDB
- raw payloads are preserved in PostgreSQL
- envelope/index rows are written to a hypertable
- scanheader rows are exposed through the `eovsa_hbin_envelope` view

Example replay/backfill scaffold:

```python
from eovsapy.historian import (
    FileSystemRawFrameStore,
    HybridHistorianSink,
    InfluxDBEnvelopeStore,
    backfill_logs_to_sink,
)

sink = HybridHistorianSink(
    FileSystemRawFrameStore("/path/to/raw-frame-store"),
    InfluxDBEnvelopeStore(),
)
count = backfill_logs_to_sink(
    sink,
    ["/path/to/sf_20260320_v66.0.log", "/path/to/sh_20260320_v66.0.log"],
)
print(count)
```

Each full-fidelity record preserves:

- frame kind: `stateframe` or `scanheader`
- embedded binary version
- XML definition version/path
- source path and frame index
- raw binary payload
- SHA-256 hash for equivalence testing
