# RangeSubselect

## Location
src/include/nodes/parsenodes.h: 615 - 621

## Overview
RangeSubselect is a parse tree node that represents a subquery appearing in a FROM clause, allowing subqueries to be used as table sources in SQL queries with optional LATERAL correlation and aliasing.

## Definition


## Detailed Description
RangeSubselect nodes are created when a subquery is used as a table source in a FROM clause. This allows complex queries to use the results of other SELECT statements as if they were tables. The structure supports the LATERAL keyword, which enables the subquery to reference columns from preceding tables in the FROM clause, creating correlated subqueries. The subquery is stored in its untransformed state and will be processed during query analysis. Optional aliasing allows the subquery results to be referenced with a table name and/or specific column names.

## Parameters / Member Variables
- : Standard NodeTag identifying this as a RangeSubselect node
- : Boolean flag indicating whether the LATERAL keyword was specified, enabling correlation with preceding FROM clause items
- : Pointer to the untransformed sub-SELECT statement that will provide the table data
- : Optional Alias structure containing the table alias name and any column aliases for the subquery results

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (inherited structure member)
  - Node (base type for subquery)
  - Alias (for table and column aliasing)
- Called from (representative examples):
  - transformRangeSubselect (src/backend/parser/parse_clause.c:407)
  - transformFromClauseItem (src/backend/parser/parse_clause.c:1083, 1089)
  - transformJsonArrayQueryConstructor (src/backend/parser/parse_expr.c:3756)
  - raw_expression_tree_walker_impl (src/backend/nodes/nodeFuncs.c:4444)

## Notes and Other Information
- RangeSubselect is part of PostgreSQL's FROM clause processing infrastructure
- LATERAL subqueries can reference columns from tables that appear earlier in the FROM clause
- The subquery is kept in untransformed state until query analysis to preserve parse tree structure
- Aliases are essential for referencing the subquery results in the outer query
- Used in complex query scenarios including derived tables, CTEs referenced in FROM clauses, and JSON query constructors
- File location: src/include/nodes/parsenodes.h:615-621