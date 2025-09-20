# transformGroupClauseList

## Location
[src/backend/parser/parse_clause.c:2475-2527](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L2475-L2527)

## Overview
Transforms a list of expressions within a GROUP BY clause or grouping set, returning a list of ressortgroupref values while safely eliminating duplicates within the clause.

## Definition

```c
static List *
transformGroupClauseList(List **flatresult,
						 ParseState *pstate, List *list,
						 List **targetlist, List *sortClause,
						 ParseExprKind exprKind, bool useSQL99, bool toplevel)
```
## Detailed Description
This function processes a list of expressions that belong to a single GROUP BY clause or grouping set. It iterates through each expression in the input list, transforms them using , and builds a result list of integer ressortgroupref values. The function maintains a local bitmap set to track already seen references within the current grouping context, allowing for safe elimination of duplicates. This is a key component in the PostgreSQL parser's handling of GROUP BY clauses and grouping sets.

## Parameters / Member Variables
- : Reference to flat list of SortGroupClause nodes that gets populated during processing
- : ParseState containing parsing context and state information
- : Input list of nodes (expressions) to be transformed
- : Reference to TargetEntry list that may be modified during transformation
- : ORDER BY clause containing SortGroupClause nodes for reference
- : ParseExprKind enum value specifying the type of expression being parsed
- : Boolean flag indicating whether to use SQL99 syntax rather than SQL92 syntax
- : Boolean flag indicating whether this is at top level (false if within any grouping set)

## Dependencies
- Functions called/Symbols referenced:
  - [transformGroupClauseExpr](transformGroupClauseExpr.md)
  - [bms_add_member](../b/bms_add_member.md)
  - lappend_int
  - [ParseExprKind](../P/ParseExprKind.md) (enum type)
- Called from (representative examples):
  - [transformGroupingSet](transformGroupingSet.md)

## Notes and Other Information
- This is a static function within parse_clause.c, indicating it's an internal helper function
- The function uses a Bitmapset to efficiently track seen local references and avoid duplicates
- Returns NIL if no valid references are found
- The function is specifically designed to handle the duplicate elimination semantics required within individual GROUP BY clauses or grouping sets
- Part of PostgreSQL's sophisticated GROUP BY and grouping sets implementation