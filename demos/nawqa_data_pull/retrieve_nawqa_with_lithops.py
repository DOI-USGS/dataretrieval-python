# Retrieve data from the National Water Quality Assessment Program (NAWQA)

import lithops
import math
import os
import pandas as pd

from dataretrieval import nldi, nwis, wqp

DESTINATION_BUCKET = os.environ.get('DESTINATION_BUCKET')
PROJECT = "National Water Quality Assessment Program (NAWQA)"


def map_retrieval(site):
    """Map function to pull data from NWIS and WQP"""
    site_list = find_neighboring_sites(site)
    # reformat for wqp
    site_list = [f"USGS-{site}" for site in site_list]

    df, _ = wqp.get_results(siteid=site_list,
                            project=PROJECT,
                            )

    # merge sites
    df['MonitoringLocationIdentifier'] = f"USGS-{site}"

    if len(df) != 0:
        df.astype(str).to_parquet(f's3://{DESTINATION_BUCKET}/nwqn-samples.parquet',
                                  engine='pyarrow',
                                  partition_cols=['MonitoringLocationIdentifier'],
                                  compression='zstd')
        # optionally, `return df` for further processing


def find_neighboring_sites(site, search_factor=0.05):
    """Find sites upstream and downstream of the given site within a certain distance.

    Parameters
    ----------
    site : str
        8-digit site number.
    search_factor : float, optional
    """
    site_df, _ = nwis.get_info(sites=site)
    drain_area_sq_mi = site_df["drain_area_va"].values[0]
    distance = _estimate_watershed_length_km(drain_area_sq_mi)

    upstream_gdf = nldi.get_features(
        feature_source="WQP",
        feature_id=f"USGS-{site}",
        navigation_mode="UM",
        distance=distance * search_factor,
        data_source="nwissite",
        )

    downstream_gdf = nldi.get_features(
        feature_source="WQP",
        feature_id=f"USGS-{site}",
        navigation_mode="DM",
        distance=distance * search_factor,
        data_source="nwissite",
        )

    features = pd.concat([upstream_gdf, downstream_gdf], ignore_index=True)

    df, _ = nwis.get_info(sites=list(features.identifier.str.strip('USGS-')))
    # drop sites with disimilar different drainage areas
    df = df.where(
        (df["drain_area_va"] / drain_area_sq_mi) > search_factor,
        ).dropna(how="all")
 
    return df["site_no"].to_list()


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
    # convert to km
    return length_miles * 1.60934


if __name__ == "__main__":
    project = "National Water Quality Assessment Program (NAWQA)"

    site_df = pd.read_csv(
        'NWQN_sites.csv',
        comment='#',
        dtype={'SITE_QW_ID': str, 'SITE_FLOW_ID': str},
        )

    site_list = site_df['SITE_QW_ID'].to_list()
    site_list = site_list[:4]  # prune for testing

    fexec = lithops.FunctionExecutor(config_file="lithops.yaml")
    futures = fexec.map(map_retrieval, site_list)

    futures.get_result()
