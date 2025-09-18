# is_redundant_with_indexclauses

## Location
src/backend/optimizer/path/equivclass.c: 3292 - 3327

## Overview
Tests whether a RestrictInfo clause is redundant with any clause in an IndexClause list by checking both simple identity and equivalence class derivation.

## Definition
```c
bool is_redundant_with_indexclauses(RestrictInfo *rinfo, List *indexclauses)
```

## Detailed Description
This function determines if a given RestrictInfo represents a condition that is redundant with any clause in a list of IndexClauses. It performs two types of redundancy checks:

1. **Simple Identity**: Direct pointer equality comparison between the input clause and IndexClause RestrictInfo
2. **Equivalence Class Derivation**: Checks if both clauses are derived from the same equivalence class

The function skips lossy index clauses since they don't enforce conditions exactly. It also notes that derived clauses in IndexClause.indexquals don't need separate checking since they would only match if their parent clause matches.

## Parameters / Member Variables
- `rinfo`: RestrictInfo structure representing the clause to test for redundancy
- `indexclauses`: List of IndexClause structures to compare against for redundancy detection

## Dependencies
- Functions called/Symbols referenced:
  - EquivalenceClass (structure type for equivalence class representation)
  - IndexClause (structure type accessed via lfirst_node macro)
  - lfirst_node (macro for safe list cell extraction)
- Called from (representative examples):
  - [extract_nonindex_conditions](../e/extract_nonindex_conditions.md) (src/backend/optimizer/path/costsize.c:851)
  - [has_indexed_join_quals](../h/has_indexed_join_quals.md) (src/backend/optimizer/path/costsize.c:5163)
  - [create_indexscan_plan](../c/create_indexscan_plan.md) (src/backend/optimizer/plan/createplan.c:3082)

## Notes and Other Information
- Combines both identity and equivalence class checks for comprehensive redundancy detection
- Skips lossy index clauses as they cannot provide exact condition enforcement
- Optimized to avoid checking derived clauses in indexquals when parent clause doesn't match
- Used extensively in index scan planning to eliminate redundant filter conditions
- Part of the query optimization process to reduce unnecessary condition evaluation