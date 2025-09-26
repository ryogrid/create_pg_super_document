# printSubscripts

## Location
[src/backend/utils/adt/ruleutils.c:12669-12698](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L12669-L12698)

## Overview
Prints array subscript expressions in SQL format by iterating through the upper and lower index expressions of a SubscriptingRef node.

## Definition
```c
static void printSubscripts(SubscriptingRef *sbsref, deparse_context *context)
```

## Detailed Description
This function generates the SQL representation of array subscript operations by processing the index expressions stored in a SubscriptingRef node. It handles both single-index subscripts (like [n]) and range subscripts (like [m:n]) by iterating through the upper and lower index expression lists.

The function formats each subscript as [lower:upper] when both bounds are present, or simply [upper] when only the upper bound exists. It properly handles NULL subexpressions by allowing get_rule_expr to print nothing for them, which is appropriate for certain array slicing operations.

## Parameters / Member Variables
- `sbsref`: The SubscriptingRef node containing the subscript expressions to print
- `context`: The deparse context containing the output buffer and decompilation state

## Dependencies
- Functions called/Symbols referenced:
  - [list_head](../l/list_head.md) (gets head of the lower index expression list)
  - [get_rule_expr](../g/get_rule_expr.md) (recursively prints each index expression)
  - [lnext](../l/lnext.md) (advances to next item in lower index list)
  - lfirst (extracts current list item)
- Called from (representative examples):
  - [get_rule_expr](../g/get_rule_expr.md)
  - [processIndirection](processIndirection.md)

## Notes and Other Information
- Handles both single subscripts [n] and range subscripts [m:n] syntax
- The reflowerindexpr list may be shorter than refupperindexpr, or may be NULL entirely
- NULL subexpressions are handled gracefully by get_rule_expr printing nothing
- Part of PostgreSQL's array subscripting syntax decompilation system
- Used in rule decompilation and query plan explanation contexts
- Formats output with square brackets and colon separators following SQL array syntax