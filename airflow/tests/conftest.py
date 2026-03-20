import sys
from pathlib import Path

DAGS_ROOT = Path(__file__).resolve().parents[1] / "dags"
if str(DAGS_ROOT) not in sys.path:
    sys.path.insert(0, str(DAGS_ROOT))
