from prefect.filesystems import GitHub
from prefect.deployments import Deployment
from etl_web_to_gcs import etl_parent_flow

github_block = GitHub.load("de-zoomcamp-github")

github_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="github-etl-deployment",
    storage=github_block,
    entrypoint="week_2/etl_web_to_gcs.py:etl_parent_flow",
    work_queue_name="default",
)


if __name__ == "__main__":
    github_dep.apply()