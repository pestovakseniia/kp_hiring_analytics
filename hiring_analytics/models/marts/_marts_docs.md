{% docs dim_shared_description %}
Dimension tables in the marts layer provide descriptive context for analytical reporting.

These tables are designed to:
- Contain cleaned and deduplicated business entities
- Provide stable surrogate keys for joins
- Enable slicing and dicing of fact data

All dimension tables follow a one-row-per-entity grain.
{% enddocs %}

{% docs dim_date_description %}
The date dimension enables time-based analysis across the hiring pipeline.

It includes calendar attributes such as:
- Day, month, year
- Week and quarter breakdowns
- Derived reporting periods

This table is used to join with fact tables on date fields for aggregations and trend analysis.
{% enddocs %}

{% docs fct_interview_description %}
The fact interview table represents the core of the hiring analytics model.

Grain:
- One row per interview

This table captures:
- Interview lifecycle timestamps
- Interview duration
- Feedback delays
- Relationships to candidates, employees, and job functions

It is used for:
- Funnel analysis
- Time-to-hire metrics
- Interview performance tracking
{% enddocs %}