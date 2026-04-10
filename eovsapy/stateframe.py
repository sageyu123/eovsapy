"""Minimal Python 3 stateframe helpers used by eovsapy.

This is not a full port of the legacy ``eovsa.stateframe`` module. It only
implements the pieces currently needed by ``eovsapy.solpnt_x``.
"""

from __future__ import annotations

import copy
import socket
import struct
from pathlib import Path
from urllib.request import urlopen

import numpy as np

from .read_xml2 import xml_ptrs
from .util import Time


def _iter_text_lines(handle):
    for line in handle:
        if isinstance(line, bytes):
            yield line.decode("utf-8", errors="replace")
        else:
            yield line


def rd_ACCfile(host=None):
    """Read key variables from ``acc.ini`` and load the stateframe template."""

    module_dir = Path(__file__).resolve().parent
    acc_path = module_dir / "acc.ini"
    xml_path = module_dir / "stateframe.xml"
    acc_lines = None
    sf = None
    version = None

    userpass = "admin:observer@"
    fqdn = socket.getfqdn()
    if fqdn.find("solar.pvt") != -1 or host == "ovsa" or fqdn.startswith("ovsa"):
        try:
            with urlopen(f"ftp://{userpass}acc.solar.pvt/ni-rt/startup/acc.ini", timeout=0.5) as acc_file:
                acc_lines = list(_iter_text_lines(acc_file))
            if xml_path.exists():
                sf, version = xml_ptrs(str(xml_path))
        except Exception:
            acc_lines = None

    if acc_lines is None:
        if not acc_path.exists():
            raise FileNotFoundError(f"Static ACC.ini not found at {acc_path}")
        if not xml_path.exists():
            raise FileNotFoundError(f"Static stateframe.xml not found at {xml_path}")
        with acc_path.open("r", encoding="utf-8", errors="replace") as acc_file:
            acc_lines = acc_file.readlines()
        sf, version = xml_ptrs(str(xml_path))

    binsize = scdport = sfport = scdsfport = None
    boffile = ""
    xmlpath = ""
    section = None
    for line in acc_lines:
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("[") and stripped.endswith("]"):
            section = stripped
            continue
        if section == "[Stateframe]":
            if stripped.startswith("bin size = "):
                binsize = int(stripped[len("bin size = "):])
            elif stripped.startswith("template path = "):
                xmlpath = stripped[len("template path = "):]
        elif section == "[Network]":
            if stripped.startswith("TCP.schedule.port = "):
                scdport = int(stripped[len("TCP.schedule.port = "):])
            elif stripped.startswith("TCP.stateframe.port = "):
                sfport = int(stripped[len("TCP.stateframe.port = "):])
            elif stripped.startswith("TCP.schedule.stateframe.port = "):
                scdsfport = int(stripped[len("TCP.schedule.stateframe.port = "):])
        elif section == "[ROACH]":
            if stripped.startswith("boffile = "):
                boffile = stripped[len("boffile = "):]

    return {
        "host": "acc.solar.pvt",
        "binsize": binsize,
        "xmlpath": xmlpath,
        "scdport": scdport,
        "sfport": sfport,
        "scdsfport": scdsfport,
        "sf": sf,
        "version": version,
        "boffile": boffile,
    }


def extract(data, key):
    """Extract a scalar or array from packed binary stateframe data."""

    if len(key) == 3:
        shape = list(reversed(key[2]))
        val = np.array(struct.unpack_from(key[0], data, key[1]))
        val.shape = shape
        return val
    return struct.unpack_from(key[0], data, key[1])[0]


def par_angle(alt, az):
    """Calculate parallactic angle from altitude and azimuth in radians."""

    lat = 37.233170 * np.pi / 180.0
    return np.arctan2(
        -np.cos(lat) * np.sin(az),
        np.sin(lat) * np.cos(alt) - np.cos(lat) * np.sin(alt) * np.cos(az),
    )


def hadec2altaz(ha, dec):
    """Convert hour angle and declination in radians to altitude and azimuth."""

    lat = 37.233170 * np.pi / 180.0
    salt = np.sin(dec) * np.sin(lat) + np.cos(dec) * np.cos(lat) * np.cos(ha)
    alt = np.arcsin(salt)
    az = np.arctan2(
        -np.cos(dec) * np.sin(ha),
        np.sin(dec) * np.cos(lat) - np.cos(dec) * np.cos(ha) * np.sin(lat),
    )
    if isinstance(az, np.ndarray):
        az = az.copy()
        az[az < 0] += 2 * np.pi
    elif az < 0:
        az += 2 * np.pi
    return alt, az


def azel_from_sqldict(sqldict, antlist=None):
    """Calculate actual/requested az-el values and tracking flags."""

    dtor = np.pi / 180.0
    if sqldict["Timestamp"][0, 0] < Time("2025-05-22").lv:
        nsolant = 13
        nant = 15
        eqant = [8, 9, 10, 12, 13]
        idx12 = np.array([11])
    else:
        nsolant = 15
        nant = 16
        eqant = [15]
        idx12 = np.array([8, 9, 10, 11, 12, 13, 14])
    if antlist is None:
        antlist = range(nant)

    az1 = copy.deepcopy(sqldict["Ante_Cont_Azimuth1"].astype("float")) / 10000.0
    try:
        az_corr = copy.deepcopy(sqldict["Ante_Cont_AzimuthPositionCorre"].astype("float")) / 10000.0
    except Exception:
        az_corr = copy.deepcopy(sqldict["Ante_Cont_AzimuthPositionCorrected"].astype("float")) / 10000.0
    el1 = copy.deepcopy(sqldict["Ante_Cont_Elevation1"].astype("float")) / 10000.0
    try:
        el_corr = copy.deepcopy(sqldict["Ante_Cont_ElevationPositionCor"].astype("float")) / 10000.0
    except Exception:
        el_corr = copy.deepcopy(sqldict["Ante_Cont_ElevationPositionCorrected"].astype("float")) / 10000.0
    az_req = copy.deepcopy(sqldict["Ante_Cont_AzimuthPosition"].astype("float")) / 10000.0
    el_req = copy.deepcopy(sqldict["Ante_Cont_ElevationPosition"].astype("float")) / 10000.0

    rm = copy.deepcopy(sqldict["Ante_Cont_RunMode"].astype("int"))
    rms = rm.shape
    rm.shape = np.prod(rms)
    good = np.where(rm == 4)[0]
    if len(good) != 0:
        az_req_alt = copy.deepcopy(sqldict["Ante_Cont_AzimuthVirtualAxis"].astype("float")) / 10000.0
        el_req_alt = copy.deepcopy(sqldict["Ante_Cont_ElevationVirtualAxis"].astype("float")) / 10000.0
        az_req.shape = el_req.shape = az_req_alt.shape = el_req_alt.shape = np.prod(rms)
        az_req[good] = copy.deepcopy(az_req_alt[good])
        el_req[good] = copy.deepcopy(el_req_alt[good])
        az_req.shape = el_req.shape = rms

    daz = copy.deepcopy(az1 - az_req)
    az_act = copy.deepcopy(az1)
    delv = copy.deepcopy(el1 - el_req)
    el_act = copy.deepcopy(el1)

    rm.shape = rms
    rm[:, idx12] = 1
    rm.shape = np.prod(rms)
    good = np.where(rm == 1)[0]
    if len(good) != 0:
        daz.shape = delv.shape = az_act.shape = el_act.shape = az1.shape = np.prod(rms)
        az_corr.shape = az_req.shape = el1.shape = el_corr.shape = el_req.shape = np.prod(rms)
        daz[good] = copy.deepcopy(az1[good] - az_corr[good])
        az_act[good] = copy.deepcopy(az_req[good] + daz[good])
        delv[good] = copy.deepcopy(el1[good] - el_corr[good])
        el_act[good] = copy.deepcopy(el_req[good] + delv[good])
        daz.shape = delv.shape = az_req.shape = el_req.shape = az_act.shape = el_act.shape = rms

    chi = par_angle(el_act * dtor, az_act * dtor)
    for iant in eqant:
        eqel, eqaz = hadec2altaz(az_act[:, iant] * dtor, el_act[:, iant] * dtor)
        chi[:, iant] = par_angle(eqel, eqaz)

    daz = az_act - az_req
    tracklim = np.array([0.0555] * nsolant + [0.0043] * (nant - nsolant))
    trackflag = np.zeros(rms, "bool")
    for i in range(rms[0]):
        trackflag[i, :] = (np.abs(daz[i, :]) <= tracklim) & (np.abs(delv[i, :]) <= tracklim)
    if nant == 15:
        trackflag[:, 14] = False

    tracksrcflag = np.ones(rms, bool)
    offsource = (
        sqldict["Ante_Cont_RAOffset"]
        + sqldict["Ante_Cont_DecOffset"]
        + sqldict["Ante_Cont_AzOffset"]
        + sqldict["Ante_Cont_ElOffset"]
    ).nonzero()
    tracksrcflag[offsource] = False

    return {
        "dAzimuth": daz,
        "ActualAzimuth": az_act,
        "RequestedAzimuth": az_req,
        "dElevation": delv,
        "ActualElevation": el_act,
        "RequestedElevation": el_req,
        "ParallacticAngle": chi / dtor,
        "TrackFlag": trackflag,
        "TrackSrcFlag": tracksrcflag,
    }
