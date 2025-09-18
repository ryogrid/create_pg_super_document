# transformWindowFuncCall

## Location
[src/backend/parser/parse_agg.c:820-1077](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_agg.c#L820-L1077)

## Overview
Completes the initial transformation of a window function call after parse_func.c recognizes it as a window function, handling window definition management and validation of window function placement within queries.

## Definition


## Detailed Description
This function performs the final stage of window function transformation by:

1. **Nesting validation**: Ensures window function calls cannot contain other window functions (nested window functions are not allowed)
2. **Context validation**: Validates that the window function appears in an allowed SQL context using a comprehensive switch statement over ParseExprKind values
3. **Window definition management**: Either links the window function to an existing window definition or creates a new one:
   - If the OVER clause specifies a window name, finds the corresponding WINDOW clause
   - Otherwise, attempts to match window properties against existing definitions to avoid duplication
   - Creates a new window definition entry if no match is found
4. **State marking**: Sets the ParseState's p_hasWindowFuncs flag to indicate the presence of window functions

The function enforces SQL standard restrictions by rejecting window functions in inappropriate contexts like WHERE clauses, JOIN conditions, CHECK constraints, and many others.

## Parameters / Member Variables
- : Current parser state containing context information and window definitions list
- : The WindowFunc node being processed, with winref field to be set
- : Window definition specifying partitioning, ordering, and framing clauses

## Dependencies
- Functions called/Symbols referenced:
  - [contain_windowfuncs](../c/contain_windowfuncs.md)
  - [locate_windowfunc](../l/locate_windowfunc.md)
  - [ParseExprKindName](../P/ParseExprKindName.md)
  - [equal](../e/equal.md)
  - lappend
  - list_length
- Called from (representative examples):
  - [ParseFuncOrColumn](../P/ParseFuncOrColumn.md)
  - [transformJsonAggConstructor](transformJsonAggConstructor.md)

## Notes and Other Information
- Unlike aggregates, only the most closely nested pstate level is considered for window functions
- The function implements comprehensive error reporting with both custom messages and standardized ParseExprKind-based messages
- Window definition deduplication logic matches similar code in optimize_window_clauses
- The extensive switch statement ensures all ParseExprKind values are handled explicitly to catch new additions at compile time