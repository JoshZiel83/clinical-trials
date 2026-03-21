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

STATUS_VALUES = (
    "RECRUITING",
    "NOT_YET_RECRUITING",
    "ACTIVE_NOT_RECRUITING",
    "ENROLLING_BY_INVITATION",
    "AVAILABLE",
)

STATUS_WHERE_CLAUSE = (
    "overall_status IN ("
    + ", ".join(f"'{s}'" for s in STATUS_VALUES)
    + ")"
)
