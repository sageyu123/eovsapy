#!/usr/bin/env python3
"""
Apply hardcoded MySQL table definitions using .netrc credentials.

Examples:
  # local admin (root) via mysqladmin alias
  python mysql_tables.py --version 70 --host localhost --netrc-host mysqladmin \
  --database eOVSA --grant-privs "SELECT, INSERT" --skip-existing --validate
  # cloud admin via cloudsqladmin alias
  python mysql_tables.py --version 70 \
  --host eovsa-db0.cgb0fabhwkos.us-west-2.rds.amazonaws.com \
  --netrc-host cloudsqladmin --database eOVSA \
  --grant-privs "SELECT" --grant-host % --skip-existing --validate
"""

import argparse
import netrc
import re
import sys

import mysql.connector
from mysql.connector import Error as MySQLError


SQL_STATEMENTS_TEMPLATE = [
    """CREATE TABLE `fV{version}_vD16` (
  `Timestamp` double NOT NULL,
  `I16` TINYINT NOT NULL,
  `Ante_Fron_Wind_State` TINYINT NOT NULL,
  `Ante_Fron_FEM_HPol_Atte_First` TINYINT NOT NULL,
  `Ante_Fron_FEM_HPol_Atte_Second` TINYINT NOT NULL,
  `Ante_Fron_FEM_Clockms` INT NOT NULL,
  `Ante_Cont_SystemClockMJDay` INT NOT NULL,
  `Ante_Cont_Azimuth1` INT NOT NULL,
  `Ante_Cont_AzimuthPositionCorre` INT NOT NULL,
  `Ante_Cont_Elevation1` INT NOT NULL,
  `Ante_Cont_ElevationPositionCor` INT NOT NULL,
  `Ante_Cont_AzimuthPosition` INT NOT NULL,
  `Ante_Cont_ElevationPosition` INT NOT NULL,
  `Ante_Cont_RunMode` TINYINT NOT NULL,
  `Ante_Cont_AzimuthVirtualAxis` INT NOT NULL,
  `Ante_Cont_ElevationVirtualAxis` INT NOT NULL,
  `Ante_Cont_RAVirtualAxis` INT NOT NULL,
  `Ante_Cont_DecVirtualAxis` INT NOT NULL,
  `Ante_Cont_RAOffset` INT NOT NULL,
  `Ante_Cont_DecOffset` INT NOT NULL,
  `Ante_Cont_AzOffset` INT NOT NULL,
  `Ante_Cont_ElOffset` INT NOT NULL,
  `Ante_Fron_FEM_HPol_Regi_Level` TINYINT NOT NULL,
  `Ante_Fron_FEM_VPol_Regi_Level` TINYINT NOT NULL,
  PRIMARY KEY (`Timestamp`,`I16`)
)""",
    """CREATE TABLE `fV{version}_vD1` (
  `Timestamp` double NOT NULL,
  `FEMA_Powe_RFSwitchStatus` TINYINT NOT NULL,
  `FEMA_Rece_LoFreqEnabled` TINYINT NOT NULL,
  `LODM_LO1A_FSeqFile` VARCHAR(32) NOT NULL,
  `DPPoffsetattn_on` TINYINT NOT NULL,
  `Sche_Data_Weat_AvgWind` float NOT NULL,
  PRIMARY KEY (`Timestamp`)
)""",
    "GRANT {grant_privs} ON `{database}`.`fV{version}_vD16` TO '{grant_user}'@'{grant_host}'",
    "GRANT {grant_privs} ON `{database}`.`fV{version}_vD1` TO '{grant_user}'@'{grant_host}'",
]


def get_netrc_credentials(netrc_host):
    auth = netrc.netrc().authenticators(netrc_host)
    if not auth:
        raise RuntimeError(f"No .netrc entry found for host '{netrc_host}'")
    username, account, password = auth
    return username, account, password


def execute_statements(cursor, statements, verbose=False, mock_run=False, skip_existing=False):
    for statement in statements:
        if verbose or mock_run:
            action = "Mocking" if mock_run else "Executing"
            for line in statement.splitlines():
                print(f"{action}: {line}")
        if not mock_run:
            try:
                cursor.execute(statement)
            except MySQLError as exc:
                if skip_existing and exc.errno == 1050:
                    if verbose:
                        print("Skipping: table already exists.")
                    continue
                raise


def validate_version(cursor, version, database):
    expected_tables = {f"fV{version}_vD16", f"fV{version}_vD1"}
    placeholders = ", ".join(["%s"] * len(expected_tables))
    query = (
        "select table_name from information_schema.tables "
        "where table_schema = %s and table_name in (" + placeholders + ")"
    )
    params = [database] + sorted(expected_tables)
    cursor.execute(query, params)
    found = {row[0] for row in cursor.fetchall()}
    missing = expected_tables - found
    if missing:
        raise RuntimeError(f"Validation failed; missing tables: {', '.join(sorted(missing))}")
    print(f"Validation passed for version {version} in database {database}.")


def latest_versions_mysql(host, netrc_host=None, database=None):
    netrc_entry = netrc_host or host
    user, account, password = get_netrc_credentials(netrc_entry)
    dbname = database or account
    cnxn = mysql.connector.connect(
        user=user, passwd=password, host=host, database=dbname
    )
    cur = cnxn.cursor()
    cur.execute("select table_name from information_schema.tables")
    names = [row[0] for row in cur.fetchall()]
    cnxn.close()

    v1 = []
    v16 = []
    for name in names:
        match = re.match(r"fV(\d{2})_vD1$", name)
        if match:
            v1.append(int(match.group(1)))
        match = re.match(r"fV(\d{2})_vD16$", name)
        if match:
            v16.append(int(match.group(1)))
    return {"vD1": max(v1) if v1 else None, "vD16": max(v16) if v16 else None}


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Execute hardcoded mysql_tables statements using .netrc credentials.",
        epilog=(
            "Examples:\n"
            "  python mysql_tables.py --version 70 --host localhost --netrc-host mysqladmin "
            "--database eOVSA --skip-existing --validate\n"
            "  python mysql_tables.py --version 70 --host "
            "eovsa-db0.cgb0fabhwkos.us-west-2.rds.amazonaws.com --netrc-host "
            "cloudsqladmin --database eOVSA --grant-host % --skip-existing --validate"
        ),
    )
    parser.add_argument(
        "--version",
        required=False,
        help="Table version number to apply (e.g., 70).",
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="Host name to match .netrc entry (default: localhost).",
    )
    parser.add_argument(
        "--netrc-host",
        default=None,
        help="Optional .netrc machine name to use instead of --host.",
    )
    parser.add_argument(
        "--database",
        default=None,
        help="Database name override (default: account from .netrc).",
    )
    parser.add_argument(
        "--grant-user",
        default="Python3",
        help="MySQL user for GRANT statements (default: Python3).",
    )
    parser.add_argument(
        "--grant-host",
        default="localhost",
        help="MySQL host pattern for GRANT statements (default: localhost).",
    )
    parser.add_argument(
        "--grant-privs",
        default="SELECT, INSERT",
        help="Privileges for GRANT statements (default: SELECT, INSERT).",
    )
    parser.add_argument(
        "--skip-grants",
        action="store_true",
        help="Skip GRANT statements.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print each statement before execution.",
    )
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Print statements without executing or committing.",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip table creation errors when tables already exist.",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate that the version tables exist after execution.",
    )
    parser.add_argument(
        "--latest",
        action="store_true",
        help="Print the latest vD1 and vD16 versions for the target host.",
    )
    args = parser.parse_args(argv)

    if not args.latest and not args.version:
        parser.error("--version is required unless --latest is set.")

    netrc_host = args.netrc_host or args.host
    username, account, password = get_netrc_credentials(netrc_host)
    database = args.database or account
    version = str(args.version) if args.version is not None else None
    statements = []
    if version is not None:
        statements = [
            stmt.format(
                version=version,
                database=database,
                grant_user=args.grant_user,
                grant_host=args.grant_host,
                grant_privs=args.grant_privs,
            )
            for stmt in SQL_STATEMENTS_TEMPLATE
        ]
        if args.skip_grants:
            statements = [stmt for stmt in statements if not stmt.startswith("GRANT ")]

    if args.latest:
        latest = latest_versions_mysql(args.host, args.host, database)
        print(f"{args.host} {latest}")
        return

    if args.mock:
        execute_statements(
            None,
            statements,
            verbose=args.verbose,
            mock_run=True,
            skip_existing=args.skip_existing,
        )
        if args.validate:
            print("Mock run; validation skipped.")
        return

    connection = mysql.connector.connect(
        user=username, passwd=password, host=args.host, database=database
    )
    print(f"Connected to database host: {args.host}")
    cursor = connection.cursor()
    try:
        execute_statements(
            cursor,
            statements,
            verbose=args.verbose,
            skip_existing=args.skip_existing,
        )
        connection.commit()
        if args.validate:
            validate_version(cursor, version, database)
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()
