# remove_useless_groupby_columns

## Location
src/backend/optimizer/plan/planner.c: 2717 - 2883

## Overview
Optimizes GROUP BY clauses by removing columns that are functionally dependent on other GROUP BY columns, specifically those made redundant by primary key constraints.

## Definition
```c
static void remove_useless_groupby_columns(PlannerInfo *root)
```

## Detailed Description
This function performs an important query optimization by eliminating redundant columns from GROUP BY clauses. The optimization is based on the mathematical principle that if a tables primary key columns are included in the GROUP BY, then all other columns from that table are functionally determined and need not be grouped explicitly.

**Algorithm Steps:**

1. **Initial Checks**: 
   - Requires at least 2 GROUP BY items
   - Skips optimization if grouping sets are present

2. **Column Analysis**:
   - Scans processed_groupClause to identify simple Var references
   - Builds bitmapsets mapping relation IDs to their grouped column numbers
   - Ignores non-Vars, outer query variables, and complex expressions

3. **Primary Key Detection**:
   - For each relation with multiple grouped columns
   - Retrieves primary key column set using get_primary_key_attnos()
   - Identifies inheritance parent tables (except partitioned tables) to avoid duplicate row issues

4. **Redundancy Identification**:
   - Determines if primary key columns are a proper subset of grouped columns
   - Marks surplus columns (those beyond primary key requirements)

5. **GROUP BY Reconstruction**:
   - Builds new GROUP BY clause excluding redundant columns
   - Preserves non-Var expressions and necessary columns

## Parameters
- `root`: PlannerInfo structure containing the querys GROUP BY information to optimize

## Dependencies
- Functions called/Symbols referenced:
  - get_sortgroupclause_tle
  - get_primary_key_attnos
  - bms_add_member, bms_membership, bms_subset_compare, bms_difference, bms_is_member
  - SortGroupClause node handling
  - FirstLowInvalidHeapAttributeNumber constant
- Called from:
  - grouping_planner
  - standard_qp_extra

## Notes and Other Information
- **Performance Benefits**: Reduces sorting overhead by eliminating unnecessary grouping columns
- **Compatibility**: Handles queries written for DBMSes that require all selected columns in GROUP BY
- **Plan Invalidation**: Automatically invalidated when primary key constraints change via relcache
- **Future Extensions**: Could potentially be extended to unique NOT NULL constraints
- **Limitation**: Currently only handles simple Var references, not complex expressions
- **Safety**: Carefully handles inheritance hierarchies and outer query variables
- Located in src/backend/optimizer/plan/planner.c:2717-2883