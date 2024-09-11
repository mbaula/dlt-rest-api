import dlt
import rest_api.typing
from rest_api import rest_api_source

config: rest_api.typing.RESTAPIConfig = {
    "client": {
        "base_url": "https://api.github.com/repos/mbaula/ViSimulate",
        "auth": {"token": dlt.secrets["sources.github.token"]},
    },
    "resource_defaults": {},
    "resources": [
        {
            "name": "issues",
            "endpoint": {
                "path": "issues",
                "params": {
                    "state": "open",
                    "per_page": 100,
                    "since": {
                        "type": "incremental",
                        "cursor_path": "updated_at",
                        "initial_value": "2022-01-01T00:00:00Z",
                    },
                }
            },
            "write_disposition": "merge",
            "primary_key": "id",
        }
    ],
}

source = rest_api_source(config)

pipeline = dlt.pipeline(
    pipeline_name="github_visimulate_issues_pipeline",
    destination="duckdb",
    dataset_name="github_visimulate_issues",
    progress="log"
)

load_info = pipeline.run(source)
print(load_info)
