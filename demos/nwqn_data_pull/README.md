# Retrieva data from the National Water Quality Network (NWQN)

This example walks through using lithops to retrieve data from every NWQN
monitoring site, then writes the results to parquet files on s3. Each
retrieval also searches the NLDI for neighboring sites with NWQN data and
merges those data. In the streamflow example, the neighborhood search is
used to progressively fill in gaps in the record by taking data from the
nearest streamgage and rescaling it by the drainage area ratio.

1. Set up a Python environment
```bash
conda create --name dataretrieval-lithops -y python=3.11
conda activate dataretrieval-lithops
pip install -r requirements.txt
```

1. Configure compute and storage backends for [lithops](https://lithops-cloud.github.io/docs/source/configuration.html).
The configuration in `lithops.yaml` uses AWS Lambda for [compute](https://lithops-cloud.github.io/docs/source/compute_config/aws_lambda.html) and AWS S3 for [storage](https://lithops-cloud.github.io/docs/source/storage_config/aws_s3.html).
To use those backends, simply edit `lithops.yaml` with your `bucket` and `execution_role`.

1. Build a runtime image for Cubed
```bash
export LITHOPS_CONFIG_FILE=$(pwd)/lithops.yaml
lithops runtime build -b aws_lambda -f Dockerfile_dataretrieval dataretrieval-runtime
```

1. Download site list
```bash
wget https://www.sciencebase.gov/catalog/file/get/655d2063d34ee4b6e05cc9e6?f=__disk__b3%2F3e%2F5b%2Fb33e5b0038f004c2a48818d0fcc88a0921f3f689 -O NWQN_sites.csv
```

1. Create a s3 bucket for the output, then set it as an environmental variable
```bash
export DESTINATION_BUCKET=<path/to/bucket>
```

1. Run the scripts
```bash
python retrieve_nwqn_samples.py

python retrieve_nwqn_streamflow.py
```

## Cleaning up
To rebuild the Litops image, delete the existing one by running
```bash
lithops runtime delete -b aws_lambda -d dataretrieval-runtime
```