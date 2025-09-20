# match_foreign_keys_to_quals

## Location
[src/backend/optimizer/plan/initsplan.c:3209-3373](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L3209-L3373)

## Overview
Matches foreign-key constraints to equivalence classes and join quals to enable more reliable selectivity estimates, especially for multiple-column FKs where independence assumptions typically fail.

## Definition

```c
void
match_foreign_keys_to_quals(PlannerInfo *root)
```
## Detailed Description
This function is a key component of PostgreSQL's cost-based query optimization that leverages foreign key semantics for better selectivity estimation. The core idea is to identify which query join conditions match equality constraints of foreign-key relationships, allowing the optimizer to make more accurate cardinality estimates than would be possible using statistical independence assumptions.

The function processes the ForeignKeyOptInfos in root->fkey_list, annotating them with information about which equivalence classes and join qualification clauses they match. It discards any ForeignKeyOptInfos that are irrelevant for the current query, ensuring that only useful foreign key information is retained for cost estimation.

The matching process involves two main strategies:
1. Matching FK columns to equivalence classes (preferred for simple inner joins)
2. Matching FK columns to "loose" join qualification clauses (for outer joins and complex conditions)

Currently, the function only retains multicolumn FKs that are fully matched to the query, though this may be relaxed in future versions to derive partial estimates.

## Parameters / Member Variables
- : PlannerInfo structure containing all global information about the query being planned, including the foreign key list to be processed

## Dependencies
- Functions called/Symbols referenced:
  - [match_eclasses_to_foreign_key_col](match_eclasses_to_foreign_key_col.md) (matches FK columns to equivalence classes)
  - [get_leftop](../g/get_leftop.md)/get_rightop (extract operands from expressions)
  - [get_commutator](../g/get_commutator.md) (finds commutator operators)
  - lappend (list manipulation)
  - ForeignKeyOptInfo (structure containing FK optimization information)
  - EquivalenceClass (structure for equivalence class management)
  - OpExpr (operator expression node)
  - RelabelType (type relabeling expression node)
  - RELOPT_BASEREL (enumeration for base relation types)
- Called from:
  - [query_planner](../q/query_planner.md) (main query planning entry point)

## Notes and Other Information
- The function performs extensive validation to ensure both the constraining and referenced relations are base relations present in the query
- It handles both direct and reverse column matches, using commutator operators when necessary
- RelabelType nodes are stripped away to reach the underlying Var nodes for proper matching
- The function prioritizes equivalence class matches over loose qualification matches
- Foreign keys linking to inheritance child relations (otherrels) are ignored
- The current implementation requires full column matching for multicolumn FKs to be retained
- This optimization is particularly valuable for star-schema and other well-normalized database designs where FK relationships are common