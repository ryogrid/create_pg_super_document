# count_rowexpr_columns

## Location
[src/backend/parser/analyze.c:1295-1336](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/analyze.c#L1295-L1336)

## Overview
Counts the number of columns contained in a ROW() expression or a variable referencing one, primarily used for providing helpful error messages.

## Definition
```c
static int
count_rowexpr_columns(ParseState *pstate, Node *expr)
```

## Detailed Description
This utility function analyzes expressions to determine if they represent a row constructor and counts the number of columns if so. It serves primarily as a diagnostic aid to generate better error messages when users mistakenly create row expressions instead of separate column values.

The function handles two main cases:
1. **Direct RowExpr**: Directly counts arguments in a ROW() expression
2. **Variable reference**: Follows variable references to subquery target lists to find underlying RowExpr

Key behavior:
- Returns -1 for non-row expressions or when unable to determine column count
- For variables, only examines RECORD-typed variables that might reference row constructors
- Specifically handles subquery references where the target list entry contains a RowExpr
- Used primarily for error reporting rather than core functionality

The function is intentionally limited in scope since its only used for hint generation. It doesnt attempt to handle all possible cases of row expression detection.

## Parameters / Member Variables
- `pstate`: Parse state containing range table and other context needed for variable resolution
- `expr`: The expression to analyze for row structure

## Dependencies
- Functions called/Symbols referenced:
  - [GetRTEByRangeTablePosn](../G/GetRTEByRangeTablePosn.md) (resolves variable references to range table entries)
  - [get_tle_by_resno](../g/get_tle_by_resno.md) (retrieves target list entries from subqueries)
  - IsA/list_length (type checking and list operations)

- Called from (representative examples):
  - [transformInsertRow](../t/transformInsertRow.md) (for generating helpful error messages about column count mismatches)

## Notes and Other Information
- Static function with limited scope - used only for error message enhancement
- Particularly useful in INSERT ... SELECT scenarios where users accidentally create row expressions
- Provides hints like "Did you accidentally use extra parentheses?" when column counts match but structure is wrong
- Not exhaustive in its detection capabilities since its only meant for hint generation
- Focuses on the most common cases where users create unintended row expressions