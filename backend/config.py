"""Central configuration, read from the environment.

Databricks Apps inject ``DATABRICKS_HOST`` and the service-principal OAuth
credentials automatically, so the SDK's default auth "just works" in the app.
For local development set ``DATABRICKS_CONFIG_PROFILE`` (e.g. fe-vm-houssem-demo).
"""

from __future__ import annotations

import os
from functools import lru_cache


class Settings:
    # ── Agent ────────────────────────────────────────────────────────
    serving_endpoint: str = os.environ.get("SERVING_ENDPOINT", "databricks-claude-opus-5")
    agent_max_tokens: int = int(os.environ.get("AGENT_MAX_TOKENS", "32000") or "32000")
    # Reasoning models (Opus 5, GPT-5, …) reject the `temperature` parameter, so
    # it is omitted unless AGENT_TEMPERATURE is explicitly set to a number.
    _temp_env = os.environ.get("AGENT_TEMPERATURE", "").strip()
    agent_temperature = float(_temp_env) if _temp_env else None

    # ── Self-provisioning target ─────────────────────────────────────
    target_catalog: str = os.environ.get("TARGET_CATALOG", "houssem_demo_catalog")
    target_schema: str = os.environ.get("TARGET_SCHEMA", "data_model_studio")
    artifacts_volume: str = os.environ.get("ARTIFACTS_VOLUME", "artifacts")
    exec_job_name: str = os.environ.get("EXEC_JOB_NAME", "data_model_studio_apply")
    exec_job_id: str = os.environ.get("EXEC_JOB_ID", "").strip()

    auto_bootstrap: bool = (os.environ.get("AUTO_BOOTSTRAP", "true").lower() == "true")

    # ── SQL warehouse (resolved lazily; see databricks_clients) ──────
    warehouse_id_env: str = (
        os.environ.get("WAREHOUSE_ID")
        or os.environ.get("DATABRICKS_WAREHOUSE_ID")
        or os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID")
        or ""
    ).strip()

    # ── Misc ─────────────────────────────────────────────────────────
    # Absolute path to the compiled frontend (Vite outputs here).
    @property
    def build_dir(self) -> str:
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return os.path.join(here, "build")

    @property
    def artifacts_volume_path(self) -> str:
        return f"/Volumes/{self.target_catalog}/{self.target_schema}/{self.artifacts_volume}"


@lru_cache(maxsize=1)
def get_settings() -> "Settings":
    return Settings()
