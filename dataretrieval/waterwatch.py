from typing import Dict, List, Union

import pandas as pd
import requests

ResponseFormat = 'json'  # json, xml

# WaterWatch won't receive any new features but it will continue to operate.
waterwatch_url = 'https://waterwatch.usgs.gov/webservices/'


def _read_json(data: Dict) -> pd.DataFrame:
    return pd.DataFrame(data).T


def get_flood_stage(
    sites: List[str] = None, fmt: str = 'DF'
) -> Union[pd.DataFrame, Dict]:
    """
    Retrieves flood stages for a list of station numbers.

    Parameters
    ----------
    sites: List of strings
        Site numbers
    fmt: ``pandas.DataFrame`` or dict
        Returned format: Default is "DF" for ``pandas.DataFrame``, else
        a dictionary is returned.

    Returns
    -------
    station_stages: ``pandas.Dataframe`` or dict
        contains station numbers and their flood stages.
        If no flood stage for a station, ``None`` is returned.

    Examples
    --------
    .. doctest::

        >> stations = ["07144100", "07144101"]
        >> res = get_flood_stage(stations, fmt="dict")  # dictionary output
        >> print(res)
        {'07144100': {'action_stage': '20',
                      'flood_stage': '22',
                      'moderate_flood_stage': '25',
                      'major_flood_stage': '26'},
         '07144101': None}
        >> print(get_flood_stage(stations))
        >> print(res)
                action_stage flood_stage moderate_flood_stage major_flood_stage
        07144100           20          22                   25                26
        07144101         None        None                 None              None
        50057000           16          20                   24                30

    """
    res = requests.get(waterwatch_url + 'floodstage', params={'format': ResponseFormat})

    if res.ok:
        json_res = res.json()
        stages = {
            site['site_no']: {k: v for k, v in site.items() if k != 'site_no'}
            for site in json_res['sites']
        }
    else:
        raise requests.RequestException(f'[{res.status_code}] - {res.reason}')

    if not sites:
        stations_stages = stages
    else:
        stations_stages = {}
        for site in sites:
            try:
                stations_stages[site] = stages[site]
            except KeyError:
                stations_stages[site] = None

    if fmt == 'dict':
        return stations_stages
    else:
        return _read_json(stations_stages)
