# parseCheckAggregates

## Location
[src/backend/parser/parse_agg.c:1078-1274](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_agg.c#L1078-L1274)

## Overview
Validates aggregate function placement and grouping correctness after query parsing is complete, checking for misplaced aggregates and improper grouping violations.

## Definition

```c
structs.
		 */
		List	   *gsets = expand_grouping_sets(qry->groupingSets, qry->groupDistinct, 4096);
```
## Detailed Description
This function performs comprehensive validation of aggregate functions and grouping in SQL queries:

1. **Grouping sets processing**: If grouping sets are present, expands them (with a 4096 limit to prevent resource issues) and finds the intersection of all sets to determine common grouping columns
2. **Range table analysis**: Scans the range table to identify JOIN entries and self-referencing CTEs, which affect subsequent processing
3. **GROUP BY clause processing**: Builds a list of acceptable grouping expressions from the GROUP BY clause, flattening join alias variables when necessary for correct equality comparisons
4. **Variable classification**: Separates simple Vars from complex expressions in grouping clauses and identifies variables common to all grouping sets for functional dependency checking
5. **Target list validation**: Checks both regular and resjunk target list elements for ungrouped variables, including those from ORDER BY and WINDOW clauses
6. **HAVING clause validation**: Applies the same ungrouped variable checks to the HAVING clause
7. **GROUPING expression finalization**: Processes GROUPING() expressions within both target lists and HAVING clauses
8. **Recursive query validation**: Enforces the SQL standard restriction that aggregate functions cannot appear in recursive terms

The function handles complex grouping scenarios including grouping sets, functional dependencies, and join alias flattening.

## Parameters / Member Variables
- : Parser state containing context information and flags like p_hasAggs
- : Query structure containing target list, GROUP BY clause, HAVING clause, and grouping sets

## Dependencies
- Functions called/Symbols referenced:
  - [expand_grouping_sets](../e/expand_grouping_sets.md)
  - [list_intersection_int](../l/list_intersection_int.md)  
  - [get_sortgroupclause_tle](../g/get_sortgroupclause_tle.md)
  - [flatten_join_alias_vars](../f/flatten_join_alias_vars.md)
  - [finalize_grouping_exprs](../f/finalize_grouping_exprs.md)
  - [check_ungrouped_columns](../c/check_ungrouped_columns.md)
  - [locate_agg_of_level](../l/locate_agg_of_level.md)
- Called from (representative examples):
  - [transformSelectStmt](../t/transformSelectStmt.md)
  - [transformDeleteStmt](../t/transformDeleteStmt.md)
  - [transformSetOperationStmt](../t/transformSetOperationStmt.md)

## Notes and Other Information
- Should only be called when aggregates, GROUP BY, HAVING, or grouping sets are present
- Most misplaced aggregates are caught earlier in transformAggregateCall, but this provides additional validation
- The 4096 grouping set limit is arbitrary but prevents pathological resource consumption
- Join alias flattening is expensive but necessary for correct variable equality determination
- Handles both simple grouping and complex grouping sets scenarios with appropriate optimizations