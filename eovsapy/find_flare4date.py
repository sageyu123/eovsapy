from util import Time
import glob
import numpy as np
from copy import copy
from time import sleep

flmdir = '/common/webplots/flaremon/'

if __name__ == "__main__":
    ''' For non-interactive use, use a backend that does not require a display
    '''
    import matplotlib
    matplotlib.use('Agg')

import matplotlib.pylab as plt

def line2plot_date(line):
    ''' Converts a flaretest time string in one line of the file to a plot_date
    '''
    cols = line.split()
    datstr3 = cols[0][:4]+'-'+cols[0][4:6]+'-'+cols[0][6:8]
    timstr = cols[1][0:2]+':'+cols[1][2:4]+':'+cols[1][4:]
    return Time(datstr3+' '+timstr).plot_date

def dict_init(nsigma=2.5, nflare=10, nbgnd=50):
    ''' Convenience routine to create an empty dictionary
    '''
    return {'data':[], 'time':[], 'bgnd':[[],[],[]], 'flaredat':[], 'flareflag':[], 'bk_vals':[], 
                   'sigma_vals':[], 'nflare':nflare, 'nbgnd':nbgnd, 'nsigma':nsigma, 'inflare':[], 
                   'flares':{'tstart':['']*0, 'tend':['']*0, 'inprog':[None]*0}}

def live_read(line, linenum, dict_in):
    ''' Takes a line of a flaretest file plus a dictionary of accumulated data and returns an update to the
        accumulated data and a flare flag.
        
        line      just the ascii line of the flaretest file to examine
        linenum   the number of this line within the flaretest file
        dict_in   has keys time, bgnd, flaredat, inflare, flareflag, nbgnd
        
        returns an updated dict_in
    '''
    bgnd = dict_in['bgnd']
    flareflag = dict_in['flareflag']
    inflare = dict_in['inflare']
    bk_vals = dict_in['bk_vals']
    sigma_vals = dict_in['sigma_vals']
    data = dict_in['data']
    t = dict_in['time']
    nflare = dict_in['nflare']
    nbgnd = dict_in['nbgnd']
    nsigma = dict_in['nsigma']
    dict_in['time'].append(line2plot_date(line))
    startline = 120
    if (dict_in['time'][-1] % 1) < 0.58333:
        # Time is before 14:00 UT, so change default limit for flares to 4 minutes
        startline = 240
    cols = line.split()
    # Skip if any of the data points are zero
    if np.float64(cols[3]) != 0 and np.float64(cols[4]) != 0 and np.float64(cols[5]) != 0:
        if len(bgnd[0]) < nbgnd:
            # Initial creation of background points at the start of the day.  
            # Add this point to background
            bgnd[0].append(np.float64(cols[3]))
            bgnd[1].append(np.float64(cols[4]))
            bgnd[2].append(np.float64(cols[5]))
            flareflag.append(0)
            inflare.append(0)
        else:
            # Do this only if the background has at least nbgnd pts
            # Get votes for this sample
            nvotes = 0   # Number of "votes" for being in a flare
            for i in range(3):
                bk = np.mean(np.array(bgnd[i]))
                sigma = np.std(np.array(bgnd[i]))
                # Determine if we should be in the flare state
                if (np.float64(cols[3+i]) - bk) > nsigma*sigma:
                    nvotes += 1
            if linenum < startline:
                # Ignore votes within 2 min (4 min in summer) of the start of a scan
                nvotes = 0
            if nvotes <=1 :
                # This point is not consistent with being in a flare, so decrement inflare (or zero)
                inflare.append(max(inflare[-1]-1,0))
                for i in range(3):
                    # Remove first background point and add new point to end
                    bgnd[i].pop(0)
                    bgnd[i].append(np.float64(cols[3+i]))
                flareflag.append(0)
            else:
                # This point is consistent with being in a flare, so increment inflare (max = nflare)
                # and save data for level check
                if inflare[-1] == 0:
                    # This is the first flare point, so initialize flaredat
                    dict_in['flaredat'] = [np.float64(cols[3:6])]
                else:
                    dict_in['flaredat'].append(np.float64(cols[3:6]))
                inflare.append(min(inflare[-1]+1,nflare))
                if inflare[-1] == nflare:
                    if len(dict_in['flaredat']) == nbgnd:
                        # The algorithm thinks it has found a flare, but check that it is not just a level change
                        print(Time(t[-1],format='plot_date').iso,'flaredat has',len(dict_in['flaredat']),'points')
                        fvotes = 0
                        for i in range(3):
                            # Compare background standard deviation with standard deviation of nbgnd pts of flare data.
                            # We need at least two votes of f_std greater than 2.5*b_std
                            b_std = np.std(np.array(bgnd[i]))
                            f_std = np.std(np.array(dict_in['flaredat'])[nflare:,i])
                            if f_std > 2.5*b_std:
                                fvotes += 1
                            print('b_mean:',np.mean(np.array(bgnd[i])), 'f_mean:', np.mean(np.array(dict_in['flaredat'])[nflare:,i]),
                                  'b_std:', np.std(np.array(bgnd[i])),  'f_std:',  np.std(np.array(dict_in['flaredat'])[nflare:,i]))
                        print('Flare votes:',fvotes)
                        if fvotes <= 1:
                            # Not a flare, so remove earlier flare flags and set background to "flare" points
                            for i in range(nbgnd):
                                flareflag[-i] = 0
                            for i in range(3):
                                bgnd[i] = np.array(dict_in['flaredat'])[:,i].tolist()
                            inflare[-1] = 0
                            flareflag.append(0)
                        else:
                            # Definitely in a flare, so set flare flag
                            flareflag.append(nvotes)
                    else:
                        # Definitely in a flare, so set flare flag
                        flareflag.append(nvotes)
                else:
                    if flareflag[-1] == 1 and inflare > nflare/2:
                        # We were in a flare state and half the flare window is not yet expired, 
                        # so even though this point may not be in the flare, go ahead and add it
                        flareflag.append(nvotes)
                    else:
                        # Half the flare window is expired, so consider this point as not in a flare
                        flareflag.append(0)
        for i in range(3):
            bk = np.mean(np.array(bgnd[i]))
            sigma = np.std(np.array(bgnd[i]))
            bk_vals.append(bk)
            sigma_vals.append(sigma)
            data.append(np.float64(cols[3+i])-bk)
    else:
        # One or more of the datapoints was 0, so set all to nan, but remain agnostic about flareflag
        data += [np.nan,np.nan,np.nan]
        bk_vals += [np.nan,np.nan,np.nan]
        sigma_vals += [np.nan,np.nan,np.nan]
        if len(flareflag) == 0:
            flareflag.append(0)
        else:
            flareflag.append(flareflag[-1])
    if len(flareflag)*3 != len(data):
        import pdb; pdb.set_trace()
        
    # Generate an "fgood" array that removes any flare flags of less than nbgnd s duration
    f_on = np.clip(np.array(flareflag),0,1)
    transitions_up, = np.where(f_on - np.roll(f_on,1) == 1)
    transitions_down, = np.where(f_on - np.roll(f_on,1) == -1)
    tup = []
    tdown = []
    for i, tran in enumerate(transitions_up):
        if np.sum(f_on[tran:tran+nbgnd-nflare]) == nbgnd-nflare:
            tup.append(transitions_up[i])
            tdown.append(transitions_down[i])
    fgood = np.zeros_like(f_on)
    for i in range(len(tup)):
        fgood[tup[i]:tdown[i]] = 1
    sidx, = np.where(fgood[1:]-fgood[:-1] == 1)
    eidx, = np.where(fgood[1:]-fgood[:-1] == -1)
    gaps = sidx[1:] - eidx[:-1]
    skip, = np.where(gaps == 1)
    for s in skip:
        fgood[sidx[s+1]] = 1    # This eliminates some 1-s gaps
    dict_in.update({'fgood':fgood})
    # Examine fgood for flares and create flare list
    sidx, = np.where(fgood[1:]-fgood[:-1] == 1)
    eidx, = np.where(fgood[1:]-fgood[:-1] == -1)
    lsidx = len(sidx)
    leidx = len(eidx)
    dict_in['flares'] = {'tstart':['']*lsidx, 'tend':['']*leidx, 'inprog':[None]*lsidx}
    for j in range(lsidx):
        dict_in['flares']['tstart'][j] = Time(dict_in['time'][sidx[j]],format='plot_date').iso
        if j == leidx:
            # This flare is still in progress
            dict_in['flares']['inprog'][j] = Time(dict_in['time'][-1],format='plot_date').iso
        else:
            dict_in['flares']['tend'][j] = Time(dict_in['time'][eidx[j]],format='plot_date').iso
            dict_in['flares']['inprog'][j] = None

    return dict_in

def date2filelist(datstr):
    ''' Return a list of flaretest files for the given date
    '''
    yyyy = datstr[:4]
    mm = datstr[5:7]
    dd = datstr[8:10]
    mjd = int(Time(datstr).mjd)+0.5
    mjd2 = mjd + 0.6
    datstr2 = Time(mjd2,format='mjd').iso[:10]
    yyyy2 = datstr2[:4]
    mm2 = datstr2[5:7]
    dd2 = datstr2[8:10]
    fstr = '/data1/eovsa/fits/FTST/'+yyyy+'/'+mm+'/flaretest_'+yyyy[2:]+mm+dd+'*.txt'
    files = sorted(glob.glob(fstr))
    fstr2 = '/data1/eovsa/fits/FTST/'+yyyy2+'/'+mm2+'/flaretest_'+yyyy2[2:]+mm2+dd2+'*.txt'
    files += sorted(glob.glob(fstr2))
    if len(files) == 0:
        print('No files (yet) for this date')
        return None
    filelist = []
    for file in files:
        s = file[-16:-4]
        mjdfile = Time('20'+s[:2]+'-'+s[2:4]+'-'+s[4:6]+' '+s[6:8]+':'+s[8:10]+':'+s[10:]).mjd
        if mjdfile > mjd and mjdfile < mjd2:
            filelist.append(file)
    if len(filelist) == 0:
        print('No files (yet) during the solar day for this date')
        return None
    return filelist
    
def plot2now(datstr=None, dict_in=None, dict_in2= None, nsigma=2.5, nflare=10, nbgnd=50, nbgnd2=200, live_plot=False):
    ''' Given a date string in the form "yyyy-mm-dd" (default is current date), finds the list of files for that
        date and analyzes them line by line to identify flares.  Creates a nice plot of the results and returns 
        a dictionary of results and a handle to the plot axis.  Call this once and then use eo_live_plot() to 
        continue plotting new data.
    '''
    if dict_in is None:
        dict_in = dict_init(nsigma=nsigma, nflare=nflare, nbgnd=nbgnd)
    if dict_in2 is None:
        dict_in2 = dict_init(nsigma=nsigma, nflare=nflare, nbgnd=nbgnd2)
    if datstr is None:
        datstr = Time.now().iso
    filelist = date2filelist(datstr)
    if filelist is None:
        return dict_in, None, dict_in2

    fig, ax = plt.subplots(2,1)
    fig.suptitle('EOVSA flare monitor for '+datstr)
    fig.set_figheight(9)
    fig.set_figwidth(14)
    ax[0].set_ylim(1000,500000)
    ax[1].set_ylim(1000,500000)

    for file in filelist:
        nline = 0
        fh = open(file,'r')
        while(1):
            # Read the most recent lines of the file in a loop:
            line = fh.readline()
            if line == '':
                # Reached end of file
                fh.close()
                break
            else:
                nline += 1
            if nline == 2: 
                project = line.split()[1].replace('\x00','')
                if project != 'NormalObserving':
                    # This is not a solar scan
                    fh.close()
                    break
            if nline > 8:
                # Main flare finding routine (works line by line)
                dict_in = live_read(line, nline-8, dict_in)
                dict_in2 = live_read(line, nline-8, dict_in2)
                if live_plot:
                    if (nline-8) % 60 == 0:
                        # Main plotting routine, plots data once per minute
                        add2plot(ax[0], dict_in)
                        add2plot(ax[1], dict_in2)
                        plt.pause(0.001)
    write_flarelist(datstr, dict_in)
    return dict_in, ax, dict_in2

def add2plot(ax, dict_in):
    ''' A plot is already open with axes ax.  This routine adds data to the axis
        by either creating the lines if they do not exist, or updating the line
        contents if they alread exist.
    '''
    t = np.array(dict_in['time'])
    tmin = int(t[0]) + 13.5/24.
    ax.set_xlim(min(t[0],tmin),max(t[-1],t[0]+1/24.))
    data = np.array(dict_in['data'])
    bk_vals = np.array(dict_in['bk_vals'])
    flareflag = np.array(dict_in['fgood'])
    if ax.lines == []:
        # First time plotting to this axis
        ax.plot_date(t,bk_vals[::3]+data[::3],'-',color='C0')
        ax.plot_date(t,bk_vals[::3],'-',color='k')
        ax.plot_date(t,bk_vals[1::3]+data[1::3],'-',color='C1')
        ax.plot_date(t,bk_vals[1::3],'-',color='k')
        ax.plot_date(t,bk_vals[2::3]+data[2::3],'-',color='C3')
        ax.plot_date(t,bk_vals[2::3],'-',color='k')
        ax.set_ylim(1000,500000)
        ax.set_yscale('log')
        if np.sum(flareflag) > 1:
            ax.fill_between(t,flareflag*500000,color='C2',alpha=0.2)
    else:
        ax.lines[0].set_data(t,bk_vals[::3]+data[::3])
        ax.lines[1].set_data(t,bk_vals[::3])
        ax.lines[2].set_data(t,bk_vals[1::3]+data[1::3])
        ax.lines[3].set_data(t,bk_vals[1::3])
        ax.lines[4].set_data(t,bk_vals[2::3]+data[2::3])
        ax.lines[5].set_data(t,bk_vals[2::3])
        if np.sum(flareflag) > 1:
            if ax.collections != []:
                ax.collections[-1].remove()
            ax.fill_between(t,flareflag*500000,color='C2',alpha=0.2)
    return

def eo_live_plot(datstr=None, nsigma=2.5, nflare=10, nbgnd=50, nbgnd2=200, live_plot=False):
    ''' Plot new flare monitor data as it is written, once per minute (will first plot
        all pre-existing data for the current date).  Detects when the data for a given
        day has ended, and starts a new day.  Runs forever, until killed.
    '''
    if datstr:
        datstr_orig = datstr
    else:
        datstr_orig = Time(Time.now().mjd - 0.5, format='mjd').iso[:10]  # Times earlier than 12 UT are considered previous date
    # This analyzes and plots all non-live data for the specified date
    dict_in, ax, dict_in2 = plot2now(datstr_orig, nsigma=nsigma, nflare=nflare, nbgnd=nbgnd, nbgnd2=nbgnd2, live_plot=live_plot)
    if len(dict_in['flares']['inprog'])>0:
        if not dict_in['flares']['inprog'][-1] is None:
            print('Flare in progress at',dict_in['flares']['inprog'][-1])
        write_flarelist(datstr_orig, dict_in)
    if ax is None:
        # There are no files (yet) for this date
        lasttime = 0.0
    else:
        # The plot now exists for all data up to now
        lasttime = dict_in['time'][-1]
    while(1):
        datstr = Time(Time.now().mjd - 0.5, format='mjd').iso[:10]   # Check for new date
        if datstr != datstr_orig:
            # This is a new observing day, so close out any existing data/plots
            if not ax is None:
                # Close the plot and set ax to None
                # First set the x axis range from 13:30 - 02:30 UT
                if len(dict_in['time']) > 0:
                    add2plot(ax[0], dict_in)
                    add2plot(ax[1], dict_in2)
                    pd = Time(datstr_orig).plot_date
                    ax[0].set_xlim(pd + 13.5/24.,pd+1+2.5/24.)
                    ax[1].set_xlim(pd + 13.5/24.,pd+1+2.5/24.)
                    print('saving file',flmdir+'FLM'+datstr_orig.replace('-','')+'.png')
                    plt.savefig(flmdir+'FLM'+datstr_orig.replace('-','')+'.png',bbox_inches='tight')
            else:
                if len(dict_in['time']) > 0:
                    # There are data to plot
                    fig, ax = plt.subplots(2,1)
                    fig.suptitle('EOVSA flare monitor for '+datstr)
                    fig.set_figheight(5)
                    fig.set_figwidth(14)
                    ax[0].set_ylim(1000,500000)
                    ax[1].set_ylim(1000,500000)
                    add2plot(ax[0], dict_in)
                    add2plot(ax[1], dict_in2)
                    print('saving file',flmdir+'FLM'+datstr_orig.replace('-','')+'.png')
                    plt.savefig(flmdir+'FLM'+datstr_orig.replace('-','')+'.png',bbox_inches='tight')
            ax = None
            # Clear dict_in of any existing information
            dict_in = dict_init(nsigma=nsigma, nflare=nflare, nbgnd=nbgnd)
            dict_in2 = dict_init(nsigma=nsigma, nflare=nflare, nbgnd=nbgnd2)
            datstr_orig = datstr
        filelist = date2filelist(datstr)
        if filelist is None:
            # Sleep for one minute and check for a new file
            sleep(60)
        else:
            if len(dict_in['time']) == 0:
                # There are no data (yet) in the dictionary
                lasttime = 0.0
            else:
                # Note the last time in the dictionary
                lasttime = dict_in['time'][-1]
            # Check for new data in the last file in the list
            file = filelist[-1]
            nline = 0
            fh = open(file,'r')
            lines = fh.readlines()   # Just read all of the lines in the file, since the file will not grow.
            fh.close()
            updated = False
            for line in lines:
                nline += 1
                if nline == 2: 
                    project = line.split()[1].replace('\x00','')
                    if project != 'NormalObserving':
                        # This is not a solar scan, so sleep 1 minute and try again
                        sleep(60)
                        break
                if nline > 8:
                    # Main flare finding routine (works line by line)
                    if lasttime < line2plot_date(line):
                        # This line is later than the last line plotted, so update and plot it.
                        dict_in = live_read(line, nline-8, dict_in)
                        dict_in2 = live_read(line, nline-8, dict_in2)
                        if len(dict_in['flares']['inprog'])>0:
                            if type(dict_in['flares']['inprog'][-1]) is str:
                                print('Flare in progress at',dict_in['flares']['inprog'][-1])
                        write_flarelist(datstr_orig, dict_in)
                        updated = True
            # Main plotting routine, plots data if updated
            if updated:
                if ax is None:
                    fig, ax = plt.subplots(2,1)
                    fig.suptitle('EOVSA flare monitor for '+datstr)
                    fig.set_figheight(5)
                    fig.set_figwidth(14)
                    ax[0].set_ylim(1000,500000)
                    ax[1].set_ylim(1000,500000)
                add2plot(ax[0], dict_in)
                add2plot(ax[1], dict_in2)
                print('saving file',flmdir+'FLM'+datstr_orig.replace('-','')+'.png')
                plt.savefig(flmdir+'FLM'+datstr_orig.replace('-','')+'.png',bbox_inches='tight')
                sleep(60)
        print(Time.now().iso)
        write_flarelist(datstr_orig, dict_in)
            
def write_flarelist(datstr, dict_in):
    f = open(flmdir+'flarelist/flarelist_'+datstr+'.txt','w')
    f.write('____FlareID____  _____Start UTC_____  _____Peak UTC______  ______End UTC______  __Imp:_03-07__07-13__13-18__\n')
    lsidx = len(dict_in['flares']['tstart'])
    leidx = len(dict_in['flares']['tend'])
    for i in range(lsidx):
        # Find indexes of this flare
        istart, = np.where(np.array(dict_in['time']) == Time(dict_in['flares']['tstart'][i]).plot_date)
        if i == leidx:
            # This flare is still in progress
            if type(dict_in['flares']['inprog'][i]) is str:
                iend, = np.where(np.array(dict_in['time']) == Time(dict_in['flares']['inprog'][i]).plot_date)
            else:
                # Something is wrong, so just set iend to last time.
                iend = np.array([len(dict_in['time'])])
                print('Error! lsidx =',lsidx,'and leidx =',leidx,'but', dict_in['flares']['inprog'][i],'is not a Time string!')
        else:
            iend, = np.where(np.array(dict_in['time']) == Time(dict_in['flares']['tend'][i]).plot_date)
        data = np.array(dict_in['data'])
        idx = []
        flare_importance = []
        bstd0 = [2000, 500, 100]
        for j in range(3):
            flare = data[j::3][istart[0]:iend[0]]
            bgnd = data[j::3][istart[0]-dict_in['nbgnd']:istart[0]-1]
            idx.append(np.nanargmax(flare))
            nstd = np.log10((flare[idx[-1]] - np.nanmean(bgnd))/(np.nanstd(bgnd)+bstd0[j]))
            if not np.isfinite(nstd):
                nstd = 0
            flare_importance.append(int(np.clip(2*nstd,0,5)))
        ipeak = int(np.median(np.array(idx)))+istart[0]
        tpeak = Time(dict_in['time'][ipeak],format='plot_date').isot
        #print('flare:',i,'importances:',flare_importance)
        flareid = tpeak[:19].replace('-','').replace(':','')
        startutc = dict_in['flares']['tstart'][i][:19].replace(' ','T')
        peakutc = tpeak[:19]
        endutc = dict_in['flares']['tend'][i][:19].replace(' ','T')
        imp = '         {:1d}      {:1d}      {:1d}'.format(*flare_importance)
        f.write(flareid+'  '+startutc+'  '+peakutc+'  '+endutc+'  '+imp+'\n')
    f.close()
    
if __name__ == "__main__":
    ''' This will start with the beginning of the current day, and run forever until killed.
        It creates two files for each day: /common/webplots/flaremon/
    '''
    eo_live_plot()
