# dependencies_clauselist_selectivity

## Location
[src/backend/statistics/dependencies.c:1370-1829](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/dependencies.c#L1370-L1829)

## Overview
Main entry point for estimating the selectivity of a clause list using functional dependency statistics, implementing a comprehensive algorithm that finds applicable dependencies and combines selectivities while avoiding double-counting.

## Definition

```c
Selectivity
dependencies_clauselist_selectivity(PlannerInfo *root,
									List *clauses,
									int varRelid,
									JoinType jointype,
									SpecialJoinInfo *sjinfo,
									RelOptInfo *rel,
									Bitmapset **estimatedclauses)
```
## Detailed Description
This function serves as the primary interface for applying functional dependency statistics during selectivity estimation. It implements a sophisticated multi-stage algorithm:

1. **Compatibility Analysis**: Processes each clause to determine compatibility with functional dependencies, handling both simple column references and complex expressions
2. **Expression Management**: Assigns negative attribute numbers to expressions for unified processing with regular attributes, maintaining consistency across statistics objects
3. **Statistics Loading**: Loads and filters relevant dependency statistics, matching them against available clauses and expressions
4. **Attribute Remapping**: Normalizes attribute numbers across different statistics objects to enable unified processing
5. **Dependency Selection**: Iteratively finds the strongest applicable dependencies using 
6. **Selectivity Application**: Applies selected dependencies using 

The algorithm handles complex scenarios including:
- Mixed attribute and expression dependencies
- Multiple overlapping statistics objects
- Dependency chains (a→b→c)
- Expression deduplication and consistent numbering

The mathematical foundation uses the formula:


Applied recursively for multi-attribute dependencies.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context
- : List of WHERE clauses to estimate selectivity for
- : Relation ID for the target relation
- : Type of join operation being planned
- : Special join information structure
- : RelOptInfo structure containing relation statistics
- : Input/output bitmapset tracking which clauses have been estimated

## Dependencies
- Functions called/Symbols referenced:
  - [has_stats_of_kind](../h/has_stats_of_kind.md)
  - [dependency_is_compatible_clause](dependency_is_compatible_clause.md)
  - [dependency_is_compatible_expression](dependency_is_compatible_expression.md)
  - planner_rt_fetch
  - [statext_dependencies_load](../s/statext_dependencies_load.md)
  - [find_strongest_dependency](../f/find_strongest_dependency.md)
  - [clauselist_apply_dependencies](../c/clauselist_apply_dependencies.md)
  - [bms_membership](../b/bms_membership.md), bms_add_member, bms_del_member, bms_free
- Types used:
  - JoinType
  - [SpecialJoinInfo](../S/SpecialJoinInfo.md)
  - [MVDependencies](../M/MVDependencies.md)
  - MVDependency
  - StatisticExtInfo
  - STATS_EXT_DEPENDENCIES
- Called from (representative examples):
  - [statext_clauselist_selectivity](../s/statext_clauselist_selectivity.md)

## Notes and Other Information
- Returns 1.0 (no selectivity change) when no applicable dependencies are found
- Handles expression statistics by assigning them negative attribute numbers as pseudo-system attributes
- Performs extensive validation and normalization to ensure consistent attribute numbering across statistics objects
- Manages memory carefully with proper cleanup of allocated structures
- The algorithm prioritizes stronger/wider dependencies first to maximize accuracy
- Supports inheritance-aware statistics matching via  checking
- Efficiently filters out incompatible dependencies early to minimize processing overhead
- The function is the main orchestrator that coordinates all other dependency-related functions in the selectivity estimation pipeline