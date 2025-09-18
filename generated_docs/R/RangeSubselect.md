# RangeSubselect

## Location
[src/include/nodes/parsenodes.h:615-621](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L615-L621)

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
  - [Node](../N/Node.md) (base type for subquery)
  - [Alias](../A/Alias.md) (for table and column aliasing)
- Called from (representative examples):
  - [transformRangeSubselect](../t/transformRangeSubselect.md) (src/backend/parser/parse_clause.c:407)
  - [transformFromClauseItem](../t/transformFromClauseItem.md) (src/backend/parser/parse_clause.c:1083, 1089)
  - [transformJsonArrayQueryConstructor](../t/transformJsonArrayQueryConstructor.md) (src/backend/parser/parse_expr.c:3756)
  - [raw_expression_tree_walker_impl](../r/raw_expression_tree_walker_impl.md) (src/backend/nodes/nodeFuncs.c:4444)

## Notes and Other Information
- [RangeSubselect](RangeSubselect.md) is part of PostgreSQL's FROM clause processing infrastructure
- LATERAL subqueries can reference columns from tables that appear earlier in the FROM clause
- The subquery is kept in untransformed state until query analysis to preserve parse tree structure
- Aliases are essential for referencing the subquery results in the outer query
- Used in complex query scenarios including derived tables, CTEs referenced in FROM clauses, and JSON query constructors
- File location: src/include/nodes/parsenodes.h:615-621