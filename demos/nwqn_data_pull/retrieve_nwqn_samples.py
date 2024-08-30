# Retrieve data from the National Water Quality Assessment Program (NAWQA)

import lithops
import math
import os
import pandas as pd

from random import randint
from time import sleep
from dataretrieval import nldi, nwis, wqp

DESTINATION_BUCKET = os.environ.get('DESTINATION_BUCKET')
PROJECT = "National Water Quality Assessment Program (NAWQA)"
# some sites are not found in NLDI, avoid them for now
NOT_FOUND_SITES = [
    "15565447",  # "USGS-"
    "15292700",
]
BAD_GEOMETRY_SITES = [
    "06805500",
    "09306200",
]

BAD_NLDI_SITES = NOT_FOUND_SITES + BAD_GEOMETRY_SITES


def map_retrieval(site):
    """Map function to pull data from NWIS and WQP"""
    print(f"Retrieving samples from site {site}")
    # skip bad sites
    if site in BAD_NLDI_SITES:
        site_list = [site]
    # else query slowly
    else:
        sleep(randint(0, 5))
        site_list = find_neighboring_sites(site)

    # reformat for wqp
    site_list = [f"USGS-{site}" for site in site_list]

    df, _ = wqp_get_results(siteid=site_list,
                            project=PROJECT,
                            )

    try:
        # merge sites
        df['MonitoringLocationIdentifier'] = f"USGS-{site}"
        df.astype(str).to_parquet(f's3://{DESTINATION_BUCKET}/nwqn-samples.parquet',
                                  engine='pyarrow',
                                  partition_cols=['MonitoringLocationIdentifier'],
                                  compression='zstd')
        # optionally, `return df` for further processing

    except Exception as e:
        print(f"No samples returned from site {site}: {e}")


def exponential_backoff(max_retries=5, base_delay=1):
    """Exponential backoff decorator with configurable retries and base delay"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            attempts = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    if attempts > max_retries:
                        raise e
                    wait_time = base_delay * (2 ** attempts)
                    print(f"Retrying in {wait_time} seconds...")
                    sleep(wait_time)
        return wrapper
    return decorator


@exponential_backoff(max_retries=5, base_delay=1)
def nwis_get_info(*args, **kwargs):
    return nwis.get_info(*args, **kwargs)


@exponential_backoff(max_retries=5, base_delay=1)
def wqp_get_results(*args, **kwargs):
    return wqp.get_results(*args, **kwargs)


@exponential_backoff(max_retries=3, base_delay=1)
def find_neighboring_sites(site, search_factor=0.1, fudge_factor=3.0):
    """Find sites upstream and downstream of the given site within a certain distance.

    TODO Use geoconnex to determine mainstem length

    Parameters
    ----------
    site : str
        8-digit site number.
    search_factor : float, optional
        The factor by which to multiply the watershed length to determine the
        search distance.
    fudge_factor : float, optional
        An additional fudge factor to apply to the search distance, because
        watersheds are not circular.
    """
    site_df, _ = nwis_get_info(sites=site)
    drain_area_sq_mi = site_df["drain_area_va"].values[0]
    length = _estimate_watershed_length_km(drain_area_sq_mi)
    search_distance = length * search_factor * fudge_factor
    # clip between 1 and 9999km
    search_distance = max(1.0, min(9999.0, search_distance))

    # get upstream and downstream sites
    gdfs = [
        nldi.get_features(
            feature_source="WQP",
            feature_id=f"USGS-{site}",
            navigation_mode=mode,
            distance=search_distance,
            data_source="nwissite",
            )
        for mode in ["UM", "DM"]  # upstream and downstream
    ]

    features = pd.concat(gdfs, ignore_index=True)

    df, _ = nwis_get_info(sites=list(features.identifier.str.strip('USGS-')))
    # drop sites with disimilar different drainage areas
    df = df.where(
        (df["drain_area_va"] / drain_area_sq_mi) > search_factor,
        ).dropna(how="all")

    site_list = df["site_no"].to_list()

    # include the original search site among the neighbors
    if site not in site_list:
        site_list.append(site)

    return site_list


def _estimate_watershed_length_km(drain_area_sq_mi):
    """Estimate the diameter assuming a circular watershed.

    Parameters
    ----------
    drain_area_sq_mi : float
        The drainage area in square miles.

    Returns
    -------
    float
        The diameter of the watershed in kilometers.
    """
    # assume a circular watershed
    length_miles = 2 * (drain_area_sq_mi / math.pi) ** 0.5
    # convert from miles to km
    return length_miles * 1.60934


if __name__ == "__main__":
    project = "National Water Quality Assessment Program (NAWQA)"

    site_df = pd.read_csv(
        'NWQN_sites.csv',
        comment='#',
        dtype={'SITE_QW_ID': str, 'SITE_FLOW_ID': str},
        )

    site_list = site_df['SITE_QW_ID'].to_list()
    #site_list = site_list[:2]  # prune for testing

    fexec = lithops.FunctionExecutor(config_file="lithops.yaml")
    futures = fexec.map(map_retrieval, site_list)

    futures.get_result()
