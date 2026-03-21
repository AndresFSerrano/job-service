from __future__ import annotations

import sys
from pathlib import Path


CLIENT_SRC = Path(__file__).resolve().parents[1] / "client" / "src"

if str(CLIENT_SRC) not in sys.path:
    sys.path.insert(0, str(CLIENT_SRC))
