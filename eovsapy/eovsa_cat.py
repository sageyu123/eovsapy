# -*- coding: utf-8 -*-
# History:
#  2014-Dec-07  DG
#    The schedule was dying today due to one of the GEOSAT satellites
#    having 0.000 for its inclination.  This was tricky to find!  The
#    code has been changed to adjust such lines to 0.0001 (and update
#    the checksum in that line).  This is a very rare occurrence...
#
#  2015-Apr-21  JV
#    The schedule was crashing due to an error in readtle b/c of a formatting error
#    in the geosat file online at http://www.celestrak.org/NORAD/elements/geo.txt.
#    Added error checking to load_geosats() function so that it prints an error w/o
#    crashing the schedule.
#  2015-May-30  DG
#    Removed unneeded import of util's Vector and datime routines.
#  2015-Oct-24  DG
#    Added Venus
#  2016-Dec-14 BC
#    Changed the method for locating the calibrator source files in load_sidereal_cats()
#  2019-Feb-26  DG
#    Added GPS satellites, now that we have L band!
#  2019-Apr-06  DG
#    Apparently I had the wrong GPS elements text file.  It should
#    have been https://celestrak.com/NORAD/elements/gps-ops.txt.  Now fixed.
#  2022-Mar-19  DG
#    Changes for Python 3
#  2024-Apr-25  DG
#    Apparently celestrak.com is now celestrak.org.  The calling sequence for the
#    files also changed, apparently (although the direct URL to text files also
#    works in some cases).  Here are the URLs:
#      GEO: https://celestrak.org/NORAD/elements/gp.php?GROUP=geo&FORMAT=tle
#      GPS: https://celestrak.org/NORAD/elements/gp.php?GROUP=gps-ops&FORMAT=tle
#      O3B: https://celestrak.org/NORAD/elements/gp.php?GROUP=other-comm&FORMAT=tle
#  2025-Aug-05  DG
#    TS Kelso limits the number of times one can access the above pages, so now
#    I write the data to make a disk copy and if later the access fails it just reads
#    from the disk copy.
# 2025-Sep-17 SY
#   Updated handling of geo.txt, gps.txt, and ob3.txt from celestrak.org:
#     - Check for a recent local copy (<24 hours old) before downloading.
#       If found, use it; otherwise, fetch a fresh copy.
#     - Added timeout to urlopen to avoid hanging if the site is unreachable.
#     - Added unified helper (get_cached_text) for cached fetch with remote refresh fallback.
#     - Updated all CelesTrak URLs from .com to .org.
#
#   Based on CelesTrak FAQ: https://celestrak.org/NORAD/documentation/gp-data-formats.php
#     - New GP data is updated only once every 2 hours.
#     - Primary domain switched to https://celestrak.org (since Apr 26, 2021).
#       Using the old .com domain causes 301 redirects, which automated scripts
#       may mishandle, leading to repeated requests and possible blocking.
#     - Excess HTTP errors (301, 403, 404) can trigger IP blocking:
#         * >100 errors in 2 hours -> temporary block
#         * >1,000 errors in a day -> firewall block requiring manual review
#
#   Note: The original code used .com URLs with no timeout. If Helios’ IP was
#   blocked by CelesTrak, loading stateframe.py could take several minutes.
#   Although a cache existed, the logic always tried downloading first, so
#   caching did not actually prevent repeated requests or avoid blocks.


import aipy, ephem, numpy
from math import cos, sin
from numpy import pi, mat
from .readvla import readvlacaldb
from .eovsa_array import *
import urllib.request, urllib.error, urllib.parse
import re
import os
import time
try:
    from importlib.resources import files as resource_files
except ImportError:
    from importlib_resources import files as resource_files

global lat
lat = 37.233170*numpy.pi/180       # OVSA Latitude (radians)

class RadioGeosat(aipy.phs.RadioBody, object):
    '''A geostationary satellite.  Combines ephem versions of these objects with RadioBody.
       Modeled off of aipy.phs class RadioSpecial (which is for major Solar System bodies).
    '''
    def __init__(self,geosat_body, mfreq=.150,
            ionref=(0.,0.), srcshape=(0.,0.,0.), **kwargs):
        """`name' is used to lookup appropriate ephem celestial object."""
        aipy.phs.RadioBody.__init__(self, geosat_body.name, mfreq, ionref, srcshape)
        self.Body = geosat_body
    def __getattr__(self, nm):
        """First try to access attribute from this class, but if that fails, 
        try to get it from the underlying ephem object."""
        try: return object.__getattr__(self, nm)
        except(AttributeError): return self.Body.__getattribute__(nm)
    def __setattr__(self, nm, val):
        """First try to set attribute for this class, buf if that fails, 
        try to set it for the underlying ephem object."""
        try: object.__setattr__(self, nm, val)
        except(AttributeError): return setattr(self.Body, nm, val)
    def compute(self, observer):
        self.Body.compute(observer)
        aipy.phs.RadioBody.compute(self, observer)

def get_cached_text(url, cache_path, max_age_hours=24.0, timeout=10, err_msg=None):
    """
    Return file lines from a local cache if fresh, else download and refresh cache.
    Uses mtime for freshness. Returns a list of lines (with \n).
    """
    # 1) Try fresh cache first
    try:
        mtime = os.stat(cache_path).st_mtime
        if (time.time() - mtime) / 3600.0 <= max_age_hours:
            with open(cache_path, 'r', encoding='utf-8') as f:
                return f.readlines()
    except OSError:
        pass  # cache missing or unreadable

    # 2) Cache stale/missing: try to download
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'eovsapy/1.0'})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read().decode('utf-8')
        # atomic-ish write
        tmp = cache_path + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as fout:
            fout.write(data)
        os.replace(tmp, cache_path)
        return data.splitlines(True)  # keep line breaks

    except urllib.error.HTTPError as e:
        print('%s: HTTP error %d %s' % (err_msg or 'Download failed', e.code, e.reason))
    except urllib.error.URLError as e:
        print('%s: URL error %s' % (err_msg or 'Download failed', e.reason))
    except Exception as e:
        print('%s: Other error %s' % (err_msg or 'Download failed', e))

    # 3) Download failed: fall back to whatever cache exists (even if stale)
    if os.path.exists(cache_path):
        print('Warning: using stale cache %s' % cache_path)
        with open(cache_path, 'r', encoding='utf-8') as f:
            return f.readlines()

    # 4) Nothing available
    print('Error: no data available for %s and no cache %s' % (url, cache_path))
    return []

def load_geosats():
    ''' Read the list of geostationary satellites from the Celestrak site and create a list
        of RadioGeosat objects containing all satellites. (List contains 399 sats as of 6/19/14.)
    '''
    # Retrieve TLE file for geostationary satellites from Celestrak site.

    # try:
    #     f = urllib2.urlopen('https://celestrak.org/NORAD/elements/gp.php?GROUP=geo&FORMAT=tle', timeout=10)
    #     lines = f.readlines()
    #     fout = open('geo.txt','w')
    #     for line in lines:
    #         fout.write(line)
    #     fout.close()
    # except urllib2.URLError as err:
    #     print 'Error reading GEO satellite web file:', err
    #     print 'Will read from disk copy geo.txt'
    #     f = open('geo.txt','r')
    #     lines = f.readlines()
    # f.close()
    lines = get_cached_text(
        url='https://celestrak.org/NORAD/elements/gp.php?GROUP=geo&FORMAT=tle',
        cache_path='geo.txt',
        max_age_hours=24.0,
        timeout=10,
        err_msg='Error reading GEO satellite web file. Will read from disk copy geo.txt'
    )

    nlines = len(lines)
    
    # use every 3 lines to create another RadioGeosat object
    satlist = []
    for i in range(0,nlines,3):
        if lines[i+2][9:16] == ' 0.0000':
            # aa.compute() hangs for a satellite with zero inclination!
            # Change to 0.0001 degrees, and do not forget to change the checksum.
            chksum = str(int(lines[i+2][-4:]) + 1)
            lines[i+2] = lines[i+2][:9]+' 0.0001'+lines[i+2][16:-4]+chksum
        try:
            geosat_body = ephem.readtle(lines[i], lines[i+1], lines[i+2])
            src = RadioGeosat(geosat_body) # convert from an ephem Body object to a RadioGeosat object
            satlist.append(src)
        except:
            print('Error in ephem.readtle: Geosat', lines[i].strip(), 'not added to source catalog.')
    return satlist
    
def load_gpssats():
    ''' Read the list of global positioning satellites from the Celestrak site and create a list
        of RadioGeosat objects containing all satellites. (List contains 31 sats as of 2/26/2019.)
    '''
    # Retrieve TLE file for geostationary satellites from Celestrak site.
    # try:
    #     f = urllib2.urlopen('https://celestrak.org/NORAD/elements/gp.php?GROUP=gps-ops&FORMAT=tle', timeout=10)
    #     lines = f.readlines()
    #     fout = open('gps.txt','w')
    #     for line in lines:
    #         fout.write(line)
    #     fout.close()
    # except urllib2.URLError as err:
    #     print('Error reading GPS satellite web file:', err)
    #     print('Will read from disk copy gps.txt')
    #     f = open('gps.txt','r')
    #     lines = f.readlines()
    #
    # f.close()

    lines = get_cached_text(
        url='https://celestrak.org/NORAD/elements/gp.php?GROUP=gps-ops&FORMAT=tle',
        cache_path='gps.txt',
        max_age_hours=24.0,
        timeout=10,
        err_msg='Error reading GPS satellite web file. Will read from disk copy gps.txt'
    )
    nlines = len(lines)
    
    # use every 3 lines to create another RadioGeosat object
    satlist = []
    for i in range(0,nlines,3):
        if lines[i+2][9:16] == ' 0.0000':
            # aa.compute() hangs for a satellite with zero inclination!
            # Change to 0.0001 degrees, and do not forget to change the checksum.
            chksum = str(int(lines[i+2][-4:]) + 1)
            lines[i+2] = lines[i+2][:9]+' 0.0001'+lines[i+2][16:-4]+chksum
        try:
            geosat_body = ephem.readtle(lines[i], lines[i+1], lines[i+2])
            src = RadioGeosat(geosat_body) # convert from an ephem Body object to a RadioGeosat object
            satlist.append(src)
        except:
            print('Error in ephem.readtle: Geosat', lines[i].strip(), 'not added to source catalog.')
    return satlist

def load_o3bsats():
    ''' Read the list of ob3 satellites from the Celestrak site and create a list
        of RadioGeosat objects containing all satellites.  
    '''
    # Retrieve TLE file for ob3 satellites from Celestrak site.
    # try:
    #     f = urllib2.urlopen('https://celestrak.org/NORAD/elements/gp.php?GROUP=other-comm&FORMAT=tle', timeout=10)
    #     lines = f.readlines()
    #     fout = open('ob3.txt','w')
    #     for line in lines:
    #         fout.write(line)
    #     fout.close()
    # except urllib2.URLError as err:
    #     print('Error reading ob3 satellite web file:', err)
    #     print('Will read from disk copy ob3.txt')
    #     f = open('ob3.txt','r')
    #     lines = f.readlines()
    # f.close()

    lines = get_cached_text(
        url='https://celestrak.org/NORAD/elements/gp.php?GROUP=other-comm&FORMAT=tle',
        cache_path='ob3.txt',
        max_age_hours=24.0,
        timeout=10,
        err_msg='Error reading ob3 satellite web file. Will read from disk copy ob3.txt'
    )
    nlines = len(lines)
    
    # use every 3 lines to create another RadioGeosat object
    satlist = []
    for i in range(0,nlines,3):
        if lines[i+2][9:16] == ' 0.0000':
            # aa.compute() hangs for a satellite with zero inclination!
            # Change to 0.0001 degrees, and do not forget to change the checksum.
            chksum = str(int(lines[i+2][-4:]) + 1)
            lines[i+2] = lines[i+2][:9]+' 0.0001'+lines[i+2][16:-4]+chksum
        try:
            geosat_body = ephem.readtle(lines[i], lines[i+1], lines[i+2])
            src = RadioGeosat(geosat_body) # convert from an ephem Body object to a RadioGeosat object
            satlist.append(src)
        except:
            print('Error in ephem.readtle: o3bsat', lines[i].strip(), 'not added to source catalog.')
    return satlist

def load_VLAcals():
    ''' Read the list of VLA calibrators and create a list of RadioFixedBody objects containing
        all calibrators.
        
        The list contains 1865 calibrators as of 6/19/14. However, there are four with duplicate
        names so when I add them to the aipy SrcCatalog it ends up containing only 1861 VLA
        calibrators since you can't have two dictionary entries with the same key (the first one
        just gets overwritten by the second).  The sources with duplicate names are:
            '1914+166', '0354+801', '0632+159', '1300+142'
        The code to find these duplicate names is:
            src_names = [s.src_name for s in srclist]
            from collections import Counter
            [s for s,n in Counter(src_names).items() if n>1]
        I haven't looked at the calibrator list to figure out why there are duplicates - are these
        distinct sources? If someone has an issue with this they will have to figure it out.
    '''
    cal_list = readvlacaldb()
    srclist = []
    for c in cal_list:
        src = aipy.phs.RadioFixedBody(c.ra[0],c.dec[0],name=c.name,epoch='2000')
        srclist.append(src)
    return srclist

def load_sidereal_cats():
    ''' Read all files in directory Dropbox/PythonCode/Current/SourceCat with extension .srclist
        and for each line make a RadioFixedBody object and return a list of these objects.
        
        The SourceCat files should have name, RA (h:m:s), dec (d:m:s) in the first 3 columns,
        and can have an optional 4th column with flux in Jy.
    '''
    # both \r and \n can be used in files to mark new lines and sometimes both
    # so split lines based on either and don't split twice if there is more than one in a row
    from glob import glob
    filenames = glob(str(resource_files('eovsapy').joinpath('SourceCat/'))+'/*.srclist')
    lines = []
    for filen in filenames:
        f = open(filen)
        olines = f.readlines()
        f.close()
        for line in olines:
            lines.append(line.strip())
    
    srclist = []
    for l in lines:
        props = l.strip().split()
        if len(props) == 3:
            srcname, ra, dec = props
            src = aipy.phs.RadioFixedBody(ra,dec,name=srcname,epoch='2000')
        elif len(props) == 4:
            srcname, ra, dec, fluxJy = props
            src = aipy.amp.RadioFixedBody(ra,dec,name=srcname,jys=fluxJy,epoch='J2000')
        srclist.append(src)
    return srclist

def load_cat():
    ''' Create standard cat with Sun, Moon, all VLA calibrators (N~2000),
        all geosats from Celestrak (N~400),
        and all sidereal sources whose coords are listed in a .srclist file in the
        src_cat directory (so you can add a .srclist file there and it will automatically
        be added to the catalog created when this function is called).
        
        Note: this function does not run compute yet - the catalog returned is generic to all
        Observer locations.
    '''
    srclist = load_VLAcals() + load_geosats() + load_sidereal_cats() + load_o3bsats() + load_gpssats()
    
    # append Sun and Moon
    # use aipy.amp RadioSpecial objects and SrcCatalog object - they are extensions
    # of the aipy.phs classes by the same names, with the addition that they allow
    # you to set and retrieve source fluxes (in Jy)
    srclist.append(aipy.amp.RadioSpecial('Sun'))
    srclist.append(aipy.amp.RadioSpecial('Moon'))
    srclist.append(aipy.amp.RadioSpecial('Venus'))
    cat = aipy.amp.SrcCatalog(srclist)
    return cat

def eovsa_array_with_cat():
    ''' Return an aa object created by the eovsa_array module but with a source
        catalog in the .cat attribute.
    '''
    aa = eovsa_array()
    cat = load_cat()
    cat.compute(aa)
    aa.cat = cat
    return aa
