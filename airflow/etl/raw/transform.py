import json

from pathlib import Path


VOLUME_DIR = Path("/opt/airflow/volumes")


def transform_data(
    file_path: str,
    source_id_column: str = "id",
) -> str:

    transform_dir = VOLUME_DIR / "transform"
    transform_dir.mkdir(parents=True, exist_ok=True)

    source_path = Path(file_path)
    target_path = transform_dir / source_path.name

    with source_path.open(
        "r",
        encoding="utf-8",
    ) as source_file, target_path.open(
        "w",
        encoding="utf-8",
    ) as target_file:

        for line in source_file:
            record = json.loads(line)

            transformed_record = {
                "source_id": record[source_id_column],
                "payload": record,
            }

            target_file.write(
                json.dumps(
                    transformed_record,
                    ensure_ascii=False,
                )
                + "\n"
            )

    return str(target_path)