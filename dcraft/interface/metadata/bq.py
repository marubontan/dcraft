import json

from google.cloud.bigquery import Client, LoadJobConfig, SchemaField

from dcraft.domain.metadata import Metadata
from dcraft.domain.type.enum import ContentType
from dcraft.interface.metadata.base import MetadataRepository

METADATA_TABLE_SCHEMA = [
    SchemaField("id", "STRING", mode="REQUIRED"),
    SchemaField("project_name", "STRING", mode="REQUIRED"),
    SchemaField("layer", "STRING", mode="REQUIRED"),
    SchemaField("content_type", "STRING", mode="REQUIRED"),
    SchemaField("author", "STRING", mode="NULLABLE"),
    SchemaField("created_at", "DATETIME", mode="REQUIRED"),
    SchemaField("description", "STRING", mode="NULLABLE"),
    SchemaField("extra_info", "STRING", mode="NULLABLE"),
    SchemaField("source_ids", "STRING", mode="REPEATED"),
    SchemaField("format", "STRING", mode="REQUIRED"),
]


METADATA_GET_QUERY = """
SELECT *
FROM `{}.{}.{}`
WHERE id = '{}'
"""


class BqMetadataRepository(MetadataRepository):
    def __init__(self, project: str, dataset_id: str, table_id: str):
        self._project = project
        self._dataset_id = dataset_id
        self._table_id = table_id
        self._client = Client(project=project)

    def load(self, id: str) -> Metadata:
        """Loads the metadata for a specific ID.

        Parameters:
            - id (str): The ID of the metadata to load.

        Returns:
            Metadata: The loaded metadata.
        """
        query = METADATA_GET_QUERY.format(
            self._project, self._dataset_id, self._table_id, id
        )
        query_job = self._client.query(query)
        for result in query_job.result():
            metadata = Metadata(
                id=result["id"],
                project_name=result["project_name"],
                layer=result["layer"],
                content_type=ContentType[result["content_type"]],
                author=result["author"],
                created_at=result["created_at"],
                description=result["description"],
                extra_info=json.loads(result["extra_info"])
                if result["extra_info"] is not None
                else None,
                source_ids=result["source_ids"],
                format=result["format"],
            )
        return metadata

    def save(self, metadata: Metadata):
        metadata_dict = metadata.asdict
        metadata_dict["created_at"] = metadata_dict["created_at"].isoformat()
        metadata_dict["extra_info"] = (
            json.dumps(metadata_dict["extra_info"])
            if metadata_dict["extra_info"] is not None
            else None
        )
        self._client.create_dataset(self._dataset_id, exists_ok=True)
        job_config = LoadJobConfig(
            schema=METADATA_TABLE_SCHEMA, write_disposition="WRITE_APPEND"
        )
        job = self._client.load_table_from_json(
            json_rows=[metadata_dict],
            destination=f"{self._project}.{self._dataset_id}.{self._table_id}",
            job_config=job_config,
        )
        job.result()
