import pandas as pd
from tqdm import tqdm
from collections import defaultdict

_BASE_URL = "https://nwis.waterservices.usgs.gov/nwis/"

# Service name value
_SERVICE = {
    'instantaneous values': 'iv',
    'site information': 'site'
}

# Instantaneous values parameterCd mapping values
_IV_PARAMETERCD_MAPPING = {'00065': 'stage_ft',
                        '00060': 'discharge_cfs',
                        '00010': 'temperature_C'}


_FI_HDRS = set(['state', 'org', 'site', 'Discharge (cfs)', 'Stage (ft)', 'Stage (adj) (ft)', 'Date', 'Length of record (years)', 'Class', '% normal(median) (%)', '% normal(mean) (%)', 'Status'])

def fetch_current_pnw_gauge_metadata(states=['WA','OR']):
    """
    Run a query to the USGS WaterWatch webservice to retrieve streamflow gauge metadata
    and current streamflow information and then parse the query response into a pandas DataFrame
    with typecast values

    :param states: one or more 2-character USA state codes, defaults to ['WA','OR']
    :type states: str, or list thereof 
    :returns: parsed dataframe with site metadata and current streamflow data
    :rtype: pandas.DataFrame
    """    
    # Format states
    if isinstance(states, list):
        if all(isinstance(_e, str) for _e in states):
            states = ','.join(states)
    elif isinstance(states, str):
        if len(states) == 2:
            pass
        elif all(len(_e)== 2 for _e in states.split(',')):
            pass
        else:
            raise ValueError
    else:
        raise TypeError

    states = states.lower()
    # Run query
    df = pd.read_csv(f'https://waterwatch.usgs.gov/download/?gt=map&mt=real&st={states}&dt=site&ht=&fmt=csv&mk=1', index_col='id')
    # parse names and flow information 
    holder = dict(zip(_FI_HDRS, [[] for _e in _FI_HDRS]))
    idx = []
    for id, row in tqdm(df.iterrows(), total=len(df)):
        # Skip repeat header lines for multi-state query
        if id == 'id':
            continue
        # Catch station ID
        idx.append(id)
        # parse name
        nparts = row.name.split(' ')
        holder['state'].append(nparts[-1])
        holder['org'].append(nparts[0])
        holder['site'].append(' '.join(nparts[2:]))
        # parse flowinfo
        fiparts = row.flowinfo.split(';')
        fi_claimed = set()
        for _e in fiparts:
            if 'Date' in _e:
                _k = _e[:4]
                _v = pd.Timestamp(_e[6:], tz='US/Pacific')
                holder[_k].append(_v)
                fi_claimed.add(_k)
                continue
            
            try:
                _k, _v = _e.split(':')
            except:
                breakpoint()
            _v = _v.strip()
            if _k in ['Class', 'Status']:
                pass
            else:
                _vp = _v.split(' ')
                try:
                    _v = float(_vp[0])
                except:
                    breakpoint()
                if _vp[1] != '%':
                    _k += f' ({_vp[-1]})'
            holder[_k].append(_v)
            fi_claimed.add(_k)
        for _k, _v in holder.items():
            if len(_v) < len(idx):
                holder[_k].append(None)

    try:
        dfp = pd.DataFrame(holder, index=idx)
    except:
        breakpoint()
    return df.join(dfp, how='left')

