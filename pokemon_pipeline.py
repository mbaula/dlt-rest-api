import dlt
from rest_api import rest_api_source

pokemon_config = {
    "client": {
        "base_url": "https://pokeapi.co/api/v2/",
    },
    "resource_defaults": {
        "write_disposition": "replace",
        "endpoint": {
            "params": {
                "limit": 1000,
            },
        },
    },
    "resources": [
        {
            "name": "berries",
            "endpoint": {
                "path": "berry"
            },
            "selected": False
        },
        "pokemon",
        {
            "name": "berry-details",
            "endpoint": {
                "path": "berry/{berry_name}",
                "params": {
                    "berry_name": {
                        "type": "resolve",
                        "resource": "berries",
                        "field": "name",
                    },
                },
            },
        },
    ],
}

pokemon_source = rest_api_source(pokemon_config)

pipeline = dlt.pipeline(
    pipeline_name="pokemon_pipeline",
    destination="duckdb",
    dataset_name="pokemon",
    progress="log"
)

load_info = pipeline.run(pokemon_source)
print(load_info)