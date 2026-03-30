from __future__ import annotations

import sys
from pathlib import Path

import pytest

CLIENT_SRC = Path(__file__).resolve().parents[1] / "client" / "src"

if str(CLIENT_SRC) not in sys.path:
    sys.path.insert(0, str(CLIENT_SRC))


@pytest.fixture(autouse=True)
def clear_settings_cache():
    from app.core.config import get_settings
    from persistence_kit.repository_factory.factory import repository_factory as repo_factory

    get_settings.cache_clear()
    repo_factory._repo_cached.cache_clear()
    repo_factory._init_registry.cache_clear()
    repo_factory._mongo_db.cache_clear()
    yield
    get_settings.cache_clear()
    repo_factory._repo_cached.cache_clear()
    repo_factory._init_registry.cache_clear()
    repo_factory._mongo_db.cache_clear()
