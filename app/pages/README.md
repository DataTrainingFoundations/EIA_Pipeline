# Streamlit Pages

## Purpose
This folder contains the persona-specific dashboard pages that teammates use after the home page routes them into a decision flow.

## Important Files
- `grid_operations_manager.py`: short-horizon operational watchlist and alert evidence.
- `resource_planning_lead.py`: structural planning watchlist and planning trends.
- `pipeline.py`: pipeline-focused page, if present in your workflow.

## Add Something New
1. Start the page with the question the page should answer.
2. Load data through `data_access.py`, not raw SQL.
3. Keep the page flow readable: guidance, filters, data load, derived calculations, render sections.
4. Add comments where a pandas step changes the meaning of the data.
5. Update `app/README.md` if the new page changes navigation.

## Follow This Function Next
- Find the page section that renders the chart or table.
- Follow the imported data-access function.
- Trace that function back to the SQL builder in `app/data_access_*`.

## Relevant Tests
- `app/tests/test_data_access.py`

## Common Mistakes
- Repeating the same derived calculation in multiple charts.
- Mixing UI controls and data transformation code into one unreadable block.
- Rendering a chart before explaining what the user should learn from it.
