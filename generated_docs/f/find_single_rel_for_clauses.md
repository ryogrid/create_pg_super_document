# find_single_rel_for_clauses

## Location
src/backend/optimizer/path/clausesel.c: 523 - 585

## Overview
Analyzes a list of clauses to determine if they all reference exactly one relation, enabling the application of extended statistics for more accurate selectivity estimation.

## Definition
```c
static RelOptInfo *find_single_rel_for_clauses(PlannerInfo *root, List *clauses)
```

## Detailed Description
This function performs a critical analysis step in PostgreSQL's extended statistics system by determining whether a collection of clauses can benefit from single-relation extended statistics. The function implements several key behaviors:

1. **Relation Consistency Checking**: Examines each clause to ensure all clauses reference the same single relation, which is a prerequisite for applying extended statistics.

2. **RestrictInfo Processing**: Primarily works with RestrictInfo structures that contain precomputed relation membership information via the `clause_relids` bitmap.

3. **AND Clause Recursion**: Handles special cases where bare BoolExpr AND clauses appear in the list, recursively processing their arguments since the restrictinfo machinery doesn't create RestrictInfos for top-level AND expressions.

4. **Multi-relation Detection**: Uses bitmap operations to detect clauses that span multiple relations, immediately returning NULL when such clauses are found.

5. **Variable-free Clause Handling**: Skips clauses that contain no variables (constant expressions), as they don't affect extended statistics applicability.

The function is essential for the extended statistics subsystem, as extended statistics are only meaningful when applied to clauses that reference columns from the same relation.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context and relation metadata
- `clauses`: List of clauses to analyze, preferably RestrictInfo structures but with special handling for AND expressions

## Dependencies
- Functions called/Symbols referenced:
  - [is_andclause](../i/is_andclause.md)
  - [find_single_rel_for_clauses](find_single_rel_for_clauses.md) (recursive call)
  - bms_is_empty
  - [bms_get_singleton_member](../b/bms_get_singleton_member.md)
  - [find_base_rel](find_base_rel.md)
  - BoolExpr
- Called from (representative examples):
  - [clauselist_selectivity_ext](../c/clauselist_selectivity_ext.md)
  - [clauselist_selectivity_or](../c/clauselist_selectivity_or.md)
  - [find_single_rel_for_clauses](find_single_rel_for_clauses.md) (recursive)

## Notes and Other Information
This function implements a sophisticated analysis algorithm with several important characteristics:

**Return Value Semantics**:
- Returns RelOptInfo pointer if all clauses reference exactly one relation
- Returns NULL if clauses reference multiple relations, no relations, or contain unsupported clause types

**Special Case Handling**:
- Recursively processes AND clauses because restrictinfo machinery doesn't wrap them
- Ignores variable-free (constant) clauses as they don't affect extended statistics
- Handles mixed clause lists with both RestrictInfo and bare expression nodes

**Extended Statistics Integration**:
- The return value is immediately checked by callers for extended statistics eligibility
- Enables cross-column correlation analysis when all clauses target the same relation
- Critical for the effectiveness of PostgreSQL's multivariate statistics features

**Performance Considerations**:
- Uses efficient bitmap operations for relation membership testing
- Implements early termination when multi-relation clauses are detected  
- Avoids expensive pull_varnos() operations by preferring RestrictInfo structures
- The recursive nature handles nested AND expressions efficiently while maintaining single-relation constraints