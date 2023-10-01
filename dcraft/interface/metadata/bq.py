from google.cloud.bigquery import Client, DatasetReference, SchemaField, Table

from dcraft.domain.layer.base import Metadata
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
        query = METADATA_GET_QUERY.format(
            self._project, self._dataset_id, self._table_id, id
        )
        query_job = self._client.query(query)
        for result in query_job.result():
            return Metadata(
                id=result["id"],
                project_name=result["project_name"],
                layer=result["layer"],
                content_type=ContentType[result["content_type"]],
                author=result["author"],
                created_at=result["created_at"],
                description=result["description"],
                extra_info=result["extra_info"],
                source_ids=result["source_ids"],
                format=result["format"],
            )

    def save(self, metadata: Metadata):
        dataset_ref = DatasetReference(self._project, self._dataset_id)
        table_ref = dataset_ref.table(self._table_id)
        table = Table(table_ref, METADATA_TABLE_SCHEMA)
        self._client.insert_rows(table, rows=[metadata.asdict])

    def create_if_not_exist(self):
        dataset_ref = self._client.dataset(self._dataset_id)
        self._client.create_dataset(self._dataset_id, exists_ok=True)
        table_ref = dataset_ref.table(self._table_id)
        table = Table(table_ref=table_ref, schema=METADATA_TABLE_SCHEMA)
        self._client.create_table(table, exists_ok=True)
        return table_ref
