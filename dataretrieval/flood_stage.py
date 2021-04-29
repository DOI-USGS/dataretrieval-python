from typing import Dict, List

import requests

ResponseFormat = "json" # json, xml

# OLD web service dating to 2016; discontinued for new one?
url = "https://waterwatch.usgs.gov/webservices/floodstage"


def get_flood_stages(res_fmt: str = ResponseFormat) -> Dict:
    """Retrieves flood stages for all stations."""
    res = requests.get(url, params={"format": res_fmt})
    if res.ok:
        stages = res.json()
        return {site['site_no']: {k: v for k, v in site.items() if k != 'site_no'} for site in stages['sites']}


def get_flood_stage(sites: List[str]) -> Dict[str, Dict]:
    """
    Retrieves flood stages for a list of station numbers.

    Parameters
    ----------
    sites: List of strings
        Site numbers

    Returns
    -------
        Dictionary of station numbers and their flood stages. If no flood stage for a station None is returned.

    Example
    -------
    >> stations = ["07144100", "07144101"]
    >> print(get_flood_stage(stations))
    {'07144100': {'action_stage': '20', 'flood_stage': '22', 'moderate_flood_stage': '25', 'major_flood_stage': '26'}, '07144101': None}
    """
    stages = get_flood_stages()
    stations_stages = {}
    for site in sites:
        try:
            stations_stages[site] = stages[site]
        except KeyError:
            stations_stages[site] = None
    return stations_stages
