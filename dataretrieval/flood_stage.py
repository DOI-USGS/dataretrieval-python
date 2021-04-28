from typing import Dict

import requests

ResponseFormat = "json" # json, xml

url = f"https://waterwatch.usgs.gov/webservices/floodstage"


def get_flood_stages(res_fmt: str = ResponseFormat) -> Dict:
    res = requests.get(url, params={"format": res_fmt})
    if res.ok:
        stages = res.json()
        return {site['site_no']: {k: v for k, v in site.items() if k != 'site_no'} for site in stages['sites']}


def get_flood_stage(site_no: str) -> Dict:
    stages = get_flood_stages()
    return stages[site_no]
