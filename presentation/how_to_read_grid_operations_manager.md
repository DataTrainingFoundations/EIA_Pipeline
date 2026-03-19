# How to Read the Grid Operations Manager Page

## Purpose

This page helps grid operations staff answer three questions in order:

1. **What needs attention right now?**
2. **Where is the stress concentrated?**
3. **What trend explains it?**

Work top to bottom. The page is designed so that by the time you reach the trend charts, you already know which respondent to focus on.

---

## Sidebar Filters

| Filter | What it does |
|---|---|
| Date range (UTC) | Narrows all data to this window. Default is the last 7 days. |
| Respondents | Limits the page to selected respondents. Leave blank for all. |
| Display timezone | Converts timestamps in charts and tables to your local zone. Warehouse stores everything in UTC. |

---

## KPI Strip

Five numbers at the top give an immediate system health read.

| Metric | What to watch for |
|---|---|
| Respondents in scope | Confirms your filter is set as intended. |
| In alert | Any number above zero means at least one respondent has an active alert in the latest snapshot. |
| Critical priority | Respondents where a high-severity alert is active or coverage ratio has dropped below 0.80. These need triage first. |
| High-severity alerts | Count of `high` severity alert rows in the latest snapshot across all respondents. |
| Lowest coverage ratio | The minimum coverage ratio seen across all respondents. Below 0.80 is a concern. Below 0.60 is significant. |

---

## Watchlist — What Needs Attention Now?

A ranked table of all respondents in the selected window. Rows are sorted **Critical → Elevated → Stable**.

### Priority Labels

| Label | Condition |
|---|---|
| **Critical** | A high-severity alert is active, **or** coverage ratio is below 0.80 |
| **Elevated** | A medium alert is active, **or** forecast error z-score ≥ 2.5σ, **or** demand ramp z-score ≥ 2.5σ |
| **Stable** | None of the above conditions are met |

### Columns

| Column | What it means |
|---|---|
| Actual demand (MWh) | Measured load at the latest snapshot period |
| Forecast error (MWh) | Actual minus day-ahead forecast. Positive = under-forecast. Negative = over-forecast. |
| Error z-score | How unusual this forecast error is relative to the respondent's own 14-day history. ±2.5 is the alert threshold. |
| Ramp z-score | How unusual the hour-over-hour demand change is. ±2.5 is the alert threshold. |
| Coverage ratio | Total generation divided by actual demand. Below 1.0 means reported generation is less than demand. |
| Active alerts | Number of alert types currently firing for this respondent. |

> **Start here.** Pick the top Critical or Elevated respondent and use the focus selector below to drill in.

---

## Where Is Intervention Needed?

Two bar charts showing the latest snapshot.

**Top current forecast misses** — The ten respondents with the largest absolute forecast error at the latest period. Color encodes direction: red = over-forecast (demand came in lower than predicted), green = under-forecast. A large negative bar means the grid was prepared for more demand than arrived.

**Lowest current generation coverage** — The ten respondents with the lowest coverage ratio. The dashed line at 0.80 marks the threshold where a high-severity alert fires. Bars to the left of that line warrant investigation.

---

## Focus Trends

Select a respondent from the dropdown to see their hourly trends across the selected date range.

### Actual Demand vs Day-Ahead Forecast

Shows whether the respondent is consistently missing forecast in one direction, or whether errors are random. A persistent gap in one direction suggests a systematic bias worth flagging.

### Demand Ramp Stress

A dual-axis chart. The bars show ramp magnitude in MWh (left axis). The line shows the ramp z-score (right axis). The dotted lines mark the ±2.5σ alert threshold. Spikes in z-score that coincide with large ramp bars are the hours that triggered ramp alerts.

### Generation Gap and Coverage Ratio

Another dual-axis chart. The bars show generation gap in MWh — how much reported generation exceeded or fell short of actual demand. The line shows coverage ratio. Watch for extended periods where the coverage line stays below the 0.80 threshold.

---

## Active Alerts — What Evidence Supports Action?

The alert table shows all active alerts in the latest snapshot. Use the severity and type filters to narrow it down.

### Alert Types

| Alert type | What triggered it |
|---|---|
| `forecast_error` | Forecast error z-score exceeded ±2.5σ |
| `ramp_risk` | Demand ramp z-score exceeded ±2.5σ |
| `fuel_mix_support_gap` | Coverage ratio dropped below 0.90 |
| `demand_anomaly` | Demand z-score exceeded ±2.5σ |

### Severity

| Severity | Threshold |
|---|---|
| `medium` | Signal crossed the lower threshold (2.5σ or coverage < 0.90) |
| `high` | Signal crossed the escalation threshold (3.5σ or coverage < 0.80) |

### Daily Alert Volume Chart

Shows how many alerts fired each day over the selected window, broken down by severity. A rising trend means conditions are deteriorating. A falling trend means the system is stabilizing.

---

## Supporting Analysis (Expander)

Additional context available on demand — it is intentionally secondary to the watchlist.

**Fuel mix area chart** — Shows the renewable, fossil, and gas share percentages for the focus respondent over time. Use this after identifying a coverage concern to understand whether the shortfall is structural (e.g. low renewable share) or situational.

**Approximate coverage map** — Scatter plot on a US map. Bubble size represents actual demand. Color represents coverage ratio. This is approximate — coordinates are balancing authority centroids, not precise service territory polygons. Use it to spot geographic clusters of stress, not as an authoritative map.

**CSV download** — Exports the focus respondent's hourly data for the selected window. Useful for handing off to another team or doing offline analysis.

---

## What This Page Is Not

- It is not a real-time SCADA tool. Data reflects EIA published values, which have publication lags — region data is typically available within an hour; fuel type data has a lag of approximately 35 hours.
- It is not a forecasting tool. Z-scores are computed over a rolling 14-day historical window per respondent, so they reflect how unusual current conditions are relative to recent history.
- It is not a root-cause analysis tool. Use it to identify which respondent and which signal needs attention, then hand off to the appropriate operational team with the evidence from the alert table and focus trends.
