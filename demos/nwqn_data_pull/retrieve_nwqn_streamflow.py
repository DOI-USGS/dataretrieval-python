# Retrieve data from the National Water Quality Assessment Program (NAWQA)

import lithops
import os
import numpy as np
import pandas as pd


from dataretrieval import nwis
from random import randint
from time import sleep

from retrieve_nwqn_samples import find_neighboring_sites, BAD_NLDI_SITES

DESTINATION_BUCKET = os.environ.get('DESTINATION_BUCKET')
START_DATE = "1991-01-01"
END_DATE = "2023-12-31"

def map_retrieval(site):
    """Map function to pull data from NWIS and WQP"""
    print(f"Retrieving daily streamflow from site {site}")

    if site in BAD_NLDI_SITES:
        site_list = [site]
    # else query slowly
    else:
        sleep(randint(0, 5))
        site_list = find_neighboring_sites(site)

    df, _ = nwis.get_dv(
        sites=site_list,
        start=START_DATE,
        end=END_DATE,
        parameterCd="00060",
    )

    # by default, site_no is not in the index if a single site is queried
    if "site_no" in df.columns:
        index_name = df.index.names[0]
        df.set_index(["site_no", df.index], inplace=True)
        df.index.names = ["site_no", index_name]

    print(len(df), "records retrieved")
    # process the results
    if not df.empty:
        # drop rows with missing values; neglect other 00060_* columns
        df = df.dropna(subset=["00060_Mean"])
        # fill missing codes to enable string operations
        df["00060_Mean_cd"] = df["00060_Mean_cd"].fillna("M")
        df = df[df["00060_Mean_cd"].str.contains("A")]
        df['00060_Mean'] = df['00060_Mean'].replace(-999999, np.nan)

        site_info, _ = nwis.get_info(sites=site_list)
        # USACE sites may have same site_no, which creates index conflicts later
        site_info = site_info[site_info["agency_cd"] == "USGS"]  # keep only USGS sites
        site_info = site_info.set_index("site_no")

        main_site = site_info.loc[site]
        main_site_drainage_area = main_site["drain_area_va"]

        # compute fraction of drainage area
        site_info = site_info[["drain_area_va"]].copy()
        site_info["drain_fraction"] = site_info["drain_area_va"] / main_site_drainage_area
        site_info["fraction_diff"] = np.abs(1 - site_info["drain_fraction"])

        # apply drainage area fraction
        df = pd.merge(df, site_info, left_index=True, right_index=True)
        df["00060_Mean"] *= site_info.loc[df.index.get_level_values("site_no"), "drain_fraction"].values

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

        output = output.drop(columns=["drain_area_va", "drain_fraction", "fraction_diff"])
        output["site_no"] = site

    else:
        print(f"No data retrieved for site {site}")
        return

    try:
        # merge sites
        output.astype(str).to_parquet(f's3://{DESTINATION_BUCKET}/nwqn-streamflow.parquet',
                                  engine='pyarrow',
                                  partition_cols=['site_no'],
                                  compression='zstd')
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
        'NWQN_sites.csv',
        comment='#',
        dtype={'SITE_QW_ID': str, 'SITE_FLOW_ID': str},
        )

    site_list = site_df['SITE_QW_ID'].to_list()
    # site_list = site_list[:4]  # prune for testing

    fexec = lithops.FunctionExecutor(config_file="lithops.yaml")
    futures = fexec.map(map_retrieval, site_list)

    futures.get_result()
