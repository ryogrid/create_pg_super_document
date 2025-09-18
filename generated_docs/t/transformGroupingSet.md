# transformGroupingSet

## Location
src/backend/parser/parse_clause.c: 2528 - 2631

## Overview
Recursively transforms a grouping set and its content, converting expression lists into lists of ressortgrouprefs and handling nested grouping sets.

## Definition


## Detailed Description
This function processes GroupingSet nodes, which can contain various types of content including simple expression lists, nested GroupingSets, or individual expressions. It handles three main cases: (1) List nodes are transformed via  and wrapped in GROUPING_SET_SIMPLE nodes, (2) nested GroupingSet nodes are recursively processed, and (3) individual expressions are transformed via  and wrapped in simple grouping sets. The function also enforces a limit of 12 elements for CUBE operations to prevent exponential growth. GROUPING SETS within GROUPING SETS are flattened before reaching this function.

## Parameters / Member Variables
- : Reference to flat list of SortGroupClause nodes that accumulates results
- : ParseState containing parsing context and state information
- : GroupingSet node to be transformed
- : Reference to TargetEntry list that may be modified during transformation
- : ORDER BY clause containing SortGroupClause nodes for reference
- : ParseExprKind enum value specifying the type of expression being parsed
- : Boolean flag indicating whether to use SQL99 syntax rather than SQL92 syntax
- : Boolean flag indicating whether this is at top level (false if within any grouping set)

## Dependencies
- Functions called/Symbols referenced:
  - [transformGroupClauseList](transformGroupClauseList.md)
  - [transformGroupClauseExpr](transformGroupClauseExpr.md)
  - [transformGroupingSet](transformGroupingSet.md) (recursive call)
  - [makeGroupingSet](../m/makeGroupingSet.md)
  - [exprLocation](../e/exprLocation.md)
  - list_make1_int
  - GroupingSet (struct type)
  - [ParseExprKind](../P/ParseExprKind.md) (enum type)
  - GROUPING_SET_SETS, GROUPING_SET_SIMPLE, GROUPING_SET_CUBE (enum values)
- Called from (representative examples):
  - [transformGroupingSet](transformGroupingSet.md) (recursive)
  - [transformGroupClause](transformGroupClause.md)

## Notes and Other Information
- This is a static function within parse_clause.c for internal parsing operations
- Implements recursive processing to handle nested grouping set structures
- CUBE operations are artificially limited to 12 elements to prevent exponential explosion
- The function enforces that GROUPING_SET_SETS cannot appear at non-toplevel positions
- Converts expressions to ressortgroupref indices for efficient internal representation
- Part of PostgreSQL's advanced GROUP BY functionality including GROUPING SETS, CUBE, and ROLLUP