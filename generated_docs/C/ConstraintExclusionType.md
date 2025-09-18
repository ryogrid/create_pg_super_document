# ConstraintExclusionType

## Location
src/include/optimizer/cost.h: 41 - 216

## Overview
An enumeration that defines the modes for constraint exclusion optimization, which allows PostgreSQL to skip scanning tables when their constraints guarantee that no rows match the query.

## Definition


## Detailed Description
ConstraintExclusionType controls the behavior of constraint exclusion, a query optimization technique that allows the planner to skip scanning certain tables or partitions when their constraints guarantee that no rows could possibly match the query conditions. This is particularly useful in partitioned table scenarios where partition constraints can eliminate entire partitions from consideration.

The enumeration defines three distinct modes of operation, ranging from completely disabled to fully enabled, with a middle option that targets only partition scenarios.

## Parameters / Member Variables
- : Completely disables constraint exclusion optimization. No constraint-based table exclusion will occur.
- : Enables constraint exclusion for all relations. The planner will attempt to use constraints to exclude tables/partitions whenever possible.
- : Enables constraint exclusion only for partitioned table scenarios (appendrel members). This is the default setting as it provides the most benefit with the least overhead.

## Dependencies
- Functions called/Symbols referenced:
  - Cost
  - index_pages_fetched
  - Various cost calculation functions in costsize.c
- Called from (representative examples):
  - constraint_exclusion (GUC variable in plancat.c:18)
  - relation_excluded_by_constraints() in plancat.c (switch statement evaluation)
  - Configuration system in guc_tables.c

## Notes and Other Information
- This enum is used by the  GUC parameter, which can be set to 'off', 'on', or 'partition' (default)
- The 'partition' setting (CONSTRAINT_EXCLUSION_PARTITION) is recommended as it provides significant performance benefits for partitioned tables while avoiding potential performance overhead on regular tables
- When set to 'on', the system will check constraints on all relations, which can add planning overhead for non-partitioned scenarios
- The constraint exclusion logic is implemented in  in src/backend/optimizer/util/plancat.c
- This optimization is most effective with partitioned tables using range or list partitioning with non-overlapping constraints