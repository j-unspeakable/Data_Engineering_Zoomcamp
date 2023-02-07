from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

# alternative to creating GCP blocks in the UI
# insert your own service_account_file path or service_account_info dictionary from the json file
# IMPORTANT - do not store credentials in a publicly available repository!


# credentials_block = GcpCredentials(
#     service_account_info={}  # enter your credentials info or use the file method.
# )
# credentials_block.save("zoom-gcp-creds", overwrite=True)


bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load("de-zoomcamp-cred1"),

    # Insert your  GCS bucket name.
    bucket="aoj_bucket_1",  
)

bucket_block.save("de-zoomcamp-gcs", overwrite=True)