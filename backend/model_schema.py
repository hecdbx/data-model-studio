"""Canonical data-model schema shared by the API, the code generator, and the
agent. The frontend canvas serializes to exactly this shape.

Kept permissive (``extra="allow"``) so the agent can attach extra hints without
breaking validation.
"""

from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

Layer = Literal[
    "bronze", "silver", "gold",
    "fact", "dimension", "bridge",
    "hub", "link", "satellite",
    "table",
]


class ColumnRef(BaseModel):
    model_config = ConfigDict(extra="allow")
    entity: str
    column: str


class Column(BaseModel):
    model_config = ConfigDict(extra="allow")
    name: str
    type: str = "STRING"
    nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    references: Optional[ColumnRef] = None
    description: str = ""


class Entity(BaseModel):
    model_config = ConfigDict(extra="allow")
    id: str
    name: str
    catalog: Optional[str] = None
    schema_name: Optional[str] = Field(default=None, alias="schema")
    layer: Optional[Layer] = "table"
    description: str = ""
    columns: list[Column] = Field(default_factory=list)
    primary_key: list[str] = Field(default_factory=list)
    partition_by: list[str] = Field(default_factory=list)
    cluster_by: list[str] = Field(default_factory=list)
    tags: dict[str, str] = Field(default_factory=dict)
    position: Optional[dict[str, float]] = None
    is_new: bool = False
    source_pipeline: Optional[str] = None


class Relationship(BaseModel):
    model_config = ConfigDict(extra="allow")
    id: str
    from_entity: str
    from_columns: list[str] = Field(default_factory=list)
    to_entity: str
    to_columns: list[str] = Field(default_factory=list)
    type: str = "many-to-one"
    description: str = ""


class PipelineRef(BaseModel):
    model_config = ConfigDict(extra="allow")
    pipeline_id: str
    name: str = ""
    catalog: Optional[str] = None
    target: Optional[str] = None
    tables: list[str] = Field(default_factory=list)
    source_path: Optional[str] = None
    source_code: Optional[str] = None


SourceKind = Literal["uc_schemas", "volume", "sample", "blank"]


class ModelSource(BaseModel):
    model_config = ConfigDict(extra="allow")
    kind: SourceKind = "blank"
    catalog: Optional[str] = None
    schemas: list[str] = Field(default_factory=list)
    volume: Optional[str] = None

    @field_validator("kind", mode="before")
    @classmethod
    def _known_kind(cls, v: Any) -> str:
        # a kind only the canvas knows about is still a valid model: treat it as a draft
        return v if v in SourceKind.__args__ else "blank"


class DataModel(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True, protected_namespaces=())
    model_type: str = "physical"
    name: str = "data_model"
    description: str = ""
    source: ModelSource = Field(default_factory=ModelSource)
    entities: list[Entity] = Field(default_factory=list)
    relationships: list[Relationship] = Field(default_factory=list)
    pipelines: list[PipelineRef] = Field(default_factory=list)
    lineage: list[dict[str, Any]] = Field(default_factory=list)

    def entity_by_id(self, eid: str) -> Optional[Entity]:
        for e in self.entities:
            if e.id == eid or e.name == eid:
                return e
        return None


def infer_cardinality(child_pk: Optional[list[str]], fk_cols: Optional[list[str]]) -> str:
    """Cardinality of a child → parent FK. When the FK columns are the child's whole
    primary key, a parent row has at most one child (one-to-one); otherwise it's the
    usual many-to-one."""
    pk = {c.lower() for c in (child_pk or [])}
    fk = {c.lower() for c in (fk_cols or [])}
    return "one-to-one" if fk and fk == pk else "many-to-one"


def coerce_model(data: dict) -> DataModel:
    """Validate/normalize an incoming dict into a DataModel (lenient)."""
    return DataModel.model_validate(data or {})
