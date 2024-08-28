import re
from typing import List, Optional

from pydantic import BaseModel, Field
from pymongo import ASCENDING, DESCENDING, TEXT, IndexModel


def getIndexNameFromError(error_message):
    pattern = re.compile(r"Index with name: (\S+)")
    match = pattern.search(error_message)
    return match.group(1)


class Index(BaseModel):
    name: Optional[str] = Field(
        default=None,
        json_schema_extra={
            "description": "Custom name to use for this index - if none is given, a name will be generated.",
        },
    )

    fields: List[str] = Field(
        min_length=1,
        json_schema_extra={
            "description": "Fields to index. Can be prefixed with '+' or '-' to specify index direction as ascending or descending. Prefix with '$' to specify text index.",
        },
    )

    unique: Optional[bool] = Field(
        default=False,
        json_schema_extra={
            "description": "If True, creates a uniqueness constraint on the index.",
        },
    )

    sparse: Optional[bool] = Field(
        default=False,
        json_schema_extra={
            "description": "If True, omit from the index any documents that lack the indexed field.",
        },
    )

    background: Optional[bool] = Field(
        default=True,
        json_schema_extra={
            "description": "If True, this index should be created in the background.",
        },
    )

    # Used to create an expiring (TTL) collection.
    # MongoDB will automatically delete documents from this collection after <int> seconds.
    # The indexed field must be a UTC datetime or the data will not expire.
    expire_after_seconds: Optional[int] = Field(
        default=0,
        json_schema_extra={
            "description": "Used to create an expiring (TTL) collection. Documents automatically deleted after <int> seconds.",
        },
    )
    ignore_expire_after_seconds: Optional[bool] = Field(
        default=False,
        json_schema_extra={
            "description": "If True, documents will not expire after <int> seconds.",
        },
    )

    def to_pymongo(self, existing_indexes):
        # Create pymongo index models
        pymongo_fields = []
        for field in self.fields:
            # Process prefix
            direction = ASCENDING
            if field.startswith("-"):
                direction = DESCENDING
                field = field[1:]
            elif field.startswith("+"):
                field = field[1:]
            elif field.startswith("$"):
                direction = TEXT
                field = field[1:]

            pymongo_fields.append((field, direction))

        index_dict = {
            "keys": pymongo_fields,
            "unique": self.unique,
            "background": self.background,
            "sparse": self.sparse,
        }
        if not self.ignore_expire_after_seconds:
            index_dict["expireAfterSeconds"] = self.expire_after_seconds

        index = IndexModel(**index_dict)
        if index.document.get("name") in existing_indexes:
            # print(f"Index {index.document['name']} already exists. skipping this index")
            return None
        return index
