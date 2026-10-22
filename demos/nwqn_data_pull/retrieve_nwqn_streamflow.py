# Retrieve data from the National Water Quality Assessment Program (NAWQA)

import os
from random import randint
from time import sleep

import lithops
import numpy as np
import pandas as pd
from retrieve_nwqn_samples import (
    BAD_NLDI_SITES,
    find_neighboring_sites,
    get_monitoring_locations,
)

from dataretrieval import waterdata

DESTINATION_BUCKET = os.environ.get("DESTINATION_BUCKET")
START_DATE = "1991-01-01"
END_DATE = "2023-12-31"


def map_retrieval(site):
    """Map function to pull daily streamflow from the USGS Water Data API"""
    print(f"Retrieving daily streamflow from site {site}")

    if site in BAD_NLDI_SITES:
        site_list = [site]
    # else query slowly
    else:
        sleep(randint(0, 5))
        site_list = find_neighboring_sites(site)

    # Daily mean discharge, approved values only (filtered by the service).
    df, _ = waterdata.get_daily(
        monitoring_location_id=[f"USGS-{s}" for s in site_list],
        parameter_code="00060",
        statistic_id="00003",
        approval_status="Approved",
        time=f"{START_DATE}/{END_DATE}",
        skip_geometry=True,
    )

    print(len(df), "records retrieved")
    # process the results
    if not df.empty:
        # index by bare site number and day, keeping only the discharge value
        df["site_no"] = df["monitoring_location_id"].str.removeprefix("USGS-")
        df = df.set_index(["site_no", "time"])[["value"]].dropna()

        site_info, _ = get_monitoring_locations(
            monitoring_location_id=[f"USGS-{s}" for s in site_list],
            skip_geometry=True,
        )
        # keep only USGS sites; USACE sites may share a site number, which
        # creates index conflicts later
        site_info = site_info[site_info["agency_code"] == "USGS"]
        site_info["site_no"] = site_info["monitoring_location_id"].str.removeprefix(
            "USGS-"
        )
        site_info = site_info.set_index("site_no")

        main_site = site_info.loc[site]
        main_site_drainage_area = main_site["drainage_area"]

        # compute fraction of drainage area
        site_info = site_info[["drainage_area"]].copy()
        site_info["drain_fraction"] = (
            site_info["drainage_area"] / main_site_drainage_area
        )
        site_info["fraction_diff"] = np.abs(1 - site_info["drain_fraction"])

        # apply drainage area fraction
        df = pd.merge(df, site_info, left_index=True, right_index=True)
        df["value"] *= site_info.loc[
            df.index.get_level_values("site_no"), "drain_fraction"
        ].values

        # order sites by the difference in drainage area fraction
        fill_order = site_info.sort_values("fraction_diff", ascending=True)
        fill_order = fill_order.index.values

        flow_sites = df.index.get_level_values("site_no").values
        fill_order = set(fill_order).intersection(flow_sites)

        output = pd.DataFrame()

        # loop through sites and fill in missing flow values
        # going from most to least-similar drainage areas.
        for fill_site in fill_order:
            fill_data = df.loc[fill_site]
            output = update_dataframe(output, fill_data)

        output = output.drop(
            columns=["drainage_area", "drain_fraction", "fraction_diff"]
        )
        output["site_no"] = site

    else:
        print(f"No data retrieved for site {site}")
        return

    try:
        # merge sites
        output.astype(str).to_parquet(
            f"s3://{DESTINATION_BUCKET}/nwqn-streamflow.parquet",
            engine="pyarrow",
            partition_cols=["site_no"],
            compression="zstd",
        )
        # optionally, `return df` for further processing

    except Exception as e:
        print(f"Failed to write parquet: {e}")


def update_dataframe(
    original_df: pd.DataFrame,
    new_df: pd.DataFrame,
    overwrite: bool = False,
) -> pd.DataFrame:
    """Update a DataFrame with values from another DataFrame.

    NOTE: this fuction does not handle MultiIndex DataFrames.
    """
    # Identify new rows in new_df that are not in original_df
    new_rows = new_df[~new_df.index.isin(original_df.index)]

    # Concatenate new rows to original_df
    original_df = pd.concat([original_df, new_rows]).sort_index()

    return original_df


if __name__ == "__main__":
    project = "National Water Quality Assessment Program (NAWQA)"

    site_df = pd.read_csv(
        "NWQN_sites.csv",
        comment="#",
        dtype={"SITE_QW_ID": str, "SITE_FLOW_ID": str},
    )

    site_list = site_df["SITE_QW_ID"].to_list()
    # site_list = site_list[:4]  # prune for testing

    fexec = lithops.FunctionExecutor(config_file="lithops.yaml")
    futures = fexec.map(map_retrieval, site_list)

    futures.get_result()
