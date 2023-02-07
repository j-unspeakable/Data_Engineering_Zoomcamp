from prefect.filesystems import GitHub
from prefect.deployments import Deployment

github_block = GitHub.load("de-zoomcamp-github")

github_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="github-flow",
    storage=github_block
    path="week_1"
    work_queue_name="default",
)


if __name__ == "__main__":
    github_dep.apply()