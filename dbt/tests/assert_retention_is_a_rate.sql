select cohort_month, months_since_signup, retention_rate
from {{ ref('mart_cohort_retention') }}
where retention_rate < 0 or retention_rate > 1
