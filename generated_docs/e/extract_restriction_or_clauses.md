# extract_restriction_or_clauses

## Location
src/backend/optimizer/util/orclauses.c: 75 - 125

## Overview
Examines join OR-of-AND clauses to extract useful restriction OR clauses that can be applied to individual base relations, enabling early filtering during relation scans.

## Definition
```c
void extract_restriction_or_clauses(PlannerInfo *root)
```

## Detailed Description
This function performs a partial transformation toward Conjunctive Normal Form (CNF) by extracting restriction clauses from complex join OR-of-AND expressions. The key insight is that while a join clause must reference multiple relations overall, an OR of ANDs clause might contain sub-clauses that reference just one relation and can be used to build restriction clauses for that relation.

For example, given:
```sql
WHERE ((a.x = 42 AND b.y = 43) OR (a.x = 44 AND b.z = 45))
```

The function can extract additional restriction clauses:
```sql
AND (a.x = 42 OR a.x = 44)
AND (b.y = 43 OR b.z = 45)
```

These extracted clauses can be applied during base relation scans, potentially as index qualifications, reducing the number of rows that reach the join operation. The function compensates for the redundancy introduced by updating the cached selectivity of the original OR clause to maintain accurate cost estimates.

The function iterates through each base relation in the query, examines associated join clauses, and attempts to extract restriction clauses that can be safely moved to that relation using the parameterized-path machinery logic.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the query planning context and relation information

## Dependencies
- Functions called/Symbols referenced:
  - [restriction_is_or_clause](../r/restriction_is_or_clause.md)
  - [join_clause_is_movable_to](../j/join_clause_is_movable_to.md)  
  - [extract_or_clause](extract_or_clause.md)
  - [consider_new_or_clause](../c/consider_new_or_clause.md)
  - RELOPT_BASEREL (constant)
- Called from (representative examples):
  - [query_planner](../q/query_planner.md)

## Notes and Other Information
- The transformation is partial and does not completely unravel the original OR clause to avoid expression bloat
- Uses a "MAJOR HACK" to compensate for selectivity estimation issues by adjusting cached selectivity values
- The same join clause may be examined multiple times from different base relations' perspectives
- Only works with base relations (RELOPT_BASEREL), ignoring other relation types
- Relies on the parameterized-path machinery's safety checks for clause movement decisions
- The selectivity compensation mechanism depends on cached selectivities and may not work correctly with nonlinear size estimations (outer and IN joins)