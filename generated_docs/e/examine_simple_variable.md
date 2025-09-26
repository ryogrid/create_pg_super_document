# examine_simple_variable

## Location
src/backend/utils/adt/selfuncs.c: 5351 - 5617

## Overview
Handles examination of a simple Var for the examine_variable function, recursively processing variables that reference subqueries or CTEs to extract statistical information.

## Definition

```c
static void
examine_simple_variable(PlannerInfo *root, Var *var,
						VariableStatData *vardata)
```
## Detailed Description
This function is responsible for populating the statistical information in a VariableStatData structure for a simple variable reference. It handles various types of table references including regular relations, subqueries, and Common Table Expressions (CTEs).

The function operates by:
1. First checking if a custom stats hook is available and letting it handle stats acquisition
2. For regular relations (RTE_RELATION), looking up column statistics in pg_statistic and checking user permissions
3. For subqueries and CTEs, recursively analyzing the underlying query structure to extract relevant statistics
4. Handling security considerations by respecting security barriers and access permissions

The function is designed to be recursive, allowing it to drill down through multiple layers of subqueries to find the ultimate source of statistical data.

## Parameters / Member Variables
- : PlannerInfo structure containing the current planning context and query information
- : The variable (column reference) being examined for statistical information
- : Output structure that will be populated with statistical data and metadata about the variable

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache3 (for pg_statistic lookup)
  - all_rows_selectable (security permission checking)
  - bms_make_singleton (bitmap set operations)
  - find_base_rel (relation lookup)
  - get_tle_by_resno (target list entry retrieval)
  - targetIsInSortList (DISTINCT clause analysis)
  - examine_simple_variable (recursive self-call)
- Called from (representative examples):
  - examine_variable (main entry point for variable statistics examination)

## Notes and Other Information
- The function respects security barriers and row-level security policies when determining whether to expose statistical information
- For subqueries with DISTINCT clauses, it can sometimes determine uniqueness even when other statistics are unavailable
- The function handles complex cases like CTE references that may span multiple query levels
- Security considerations prevent accessing statistics from security barrier views to avoid information leakage
- The acl_ok field in vardata is set based on whether the user has permission to see all rows, affecting which statistical functions can be used later