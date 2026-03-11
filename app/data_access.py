"""Compatibility facade for business app data access imports.

The Streamlit pages continue importing from `data_access.py`, but the query
logic now lives in smaller modules so teammates can find the right function
without scanning one large file.
"""

from data_access_grid import *  # noqa: F403
from data_access_planning import *  # noqa: F403
from data_access_shared import *  # noqa: F403
from data_access_summary import *  # noqa: F403
