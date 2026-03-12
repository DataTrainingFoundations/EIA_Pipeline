# Business App

## Purpose
This folder contains the business-facing Streamlit app that reads platinum serving tables and turns them into teammate dashboards.

## Important Files
- `streamlit_app.py`: home page and navigation summary.
- `data_access.py`: compatibility facade for page imports.
- `data_access_shared.py`: DB connection and low-level SQL helper.
- `data_access_grid.py`: grid operations queries.
- `data_access_planning.py`: planning queries.
- `data_access_summary.py`: shared coverage and status queries.

## Add Something New
1. Put new SQL in the smallest matching data-access module.
2. Keep `data_access.py` as the stable import surface for pages.
3. Add the new page under `pages/`.
4. Start the page with a short explanation of what decision the page supports.
5. Add tests in `app/tests` for the query helper before wiring the page.

## Follow This Function Next
- Start in `streamlit_app.py` or a page in `pages/`.
- Then follow the imported function back into the matching `data_access_*` module.

## Relevant Tests
- `app/tests/test_data_access.py`

## Common Mistakes
- Putting SQL directly inside a page instead of a data-access module.
- Importing from a split module directly when the page should use `data_access.py`.
- Building visual logic before confirming the query and filter behavior.
