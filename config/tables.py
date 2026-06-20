"""Tables to extract from AACT and status filter definitions."""

EXTRACT_TABLES = [
    "studies",
    "designs",
    "conditions",
    "browse_conditions",
    "interventions",
    "browse_interventions",
    "sponsors",
    "keywords",
    "brief_summaries",
    "detailed_descriptions",
    "design_groups",
    "countries",
    "eligibilities",
    "calculated_values",
]

ANCHOR_TABLE = "studies"

# Documentation only. The extract no longer filters on status — it mirrors the
# full AACT cohort (A3). These are the `overall_status` values that *used* to
# define the "active/planned" subset, kept as a reference dimension (e.g. for
# analysis that wants to re-slice to active trials). NOT a filter.
ACTIVE_STATUS_VALUES = (
    "RECRUITING",
    "NOT_YET_RECRUITING",
    "ACTIVE_NOT_RECRUITING",
    "ENROLLING_BY_INVITATION",
    "AVAILABLE",
)
