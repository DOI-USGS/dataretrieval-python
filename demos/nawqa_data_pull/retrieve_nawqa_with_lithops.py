# Retrieve data from the National Water Quality Assessment Program (NAWQA)

from dataretrieval import wqp
import lithops
import os
import pandas as pd


DESTINATION_BUCKET = os.environ.get('DESTINATION_BUCKET')
PROJECT = "National Water Quality Assessment Program (NAWQA)"


def map_retrieval(site):
    """Map function to pull data from NWIS and WQP"""
    df, _ = wqp.get_results(siteid=f'USGS-{site}',
                            project=PROJECT,
                            )

    if len(df) != 0:
        df.astype(str).to_parquet(f's3://{DESTINATION_BUCKET}/nwqn-samples.parquet',
                                  engine='pyarrow',
                                  partition_cols=['MonitoringLocationIdentifier'],
                                  compression='zstd')
        # optionally, `return df` for further processing


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
