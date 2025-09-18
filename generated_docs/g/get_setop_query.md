# get_setop_query

## Location
src/backend/utils/adt/ruleutils.c: 6171 - 6318

## Overview
Recursively generates SQL text for set operation queries (UNION, INTERSECT, EXCEPT) by processing SetOperationStmt nodes and their operands.

## Definition
```c
static void get_setop_query(Node *setOp, Query *query, deparse_context *context)
```

## Detailed Description
This recursive function converts set operation parse trees back into SQL text format. It handles two main node types:

1. **RangeTblRef nodes**: Represents leaf queries in the set operation tree. The function determines when parentheses are needed based on the presence of WITH, ORDER BY, FOR UPDATE, LIMIT clauses, or nested set operations.

2. **SetOperationStmt nodes**: Represents internal nodes (UNION, INTERSECT, EXCEPT operations). The function applies intelligent parenthesization rules to minimize unnecessary parentheses while ensuring correct precedence. It avoids parentheses when the left operand is the same type of set operation.

Key features:
- Recursive processing of nested set operations
- Intelligent parenthesization based on SQL grammar rules
- Proper indentation and formatting for complex nested queries
- Handling of ALL modifier for set operations
- Stack depth checking to prevent infinite recursion
- Suppression of column names for right-hand operands (not relevant for output)

## Parameters / Member Variables
- `setOp`: Node representing either a RangeTblRef (leaf query) or SetOperationStmt (set operation)
- `query`: The parent Query structure containing the range table
- `context`: deparse_context containing formatting options and output buffer

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (prevent stack overflow in recursive calls)
  - rt_fetch (retrieve range table entry by index)
  - [get_query_def](get_query_def.md) (generate SQL for subqueries)
  - appendContextKeyword (format keywords with proper indentation)
  - nodeTag (get node type for error checking)
- Called from (representative examples):
  - [get_select_query_def](get_select_query_def.md) (src/backend/utils/adt/ruleutils.c:5722)
  - [get_setop_query](get_setop_query.md) (recursive calls at lines 6247 and 6297)

## Notes and Other Information
- Part of PostgreSQL's rule decompilation system for complex set operations
- Implements sophisticated parenthesization logic to produce clean, readable SQL
- Handles all three standard SQL set operations: UNION, INTERSECT, EXCEPT
- Supports both ALL and DISTINCT variants of set operations
- Manages proper indentation for deeply nested set operation trees
- Uses recursion with stack depth protection for safety
- Critical for view definition storage and rule system functionality