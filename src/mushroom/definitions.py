from pathlib import Path

from dagster import definitions, load_from_defs_folder

from mushroom.defs.sync import warmhub_client


@definitions
def defs():
    return load_from_defs_folder(path_within_project=Path(__file__).parent).with_resources(
        {"warmhub_client": warmhub_client}
    )
