# RangeTblFunction

## Location
src/include/nodes/parsenodes.h: 1317 - 1337

## Overview
RangeTblFunction is subsidiary data for individual functions within a FUNCTION range table entry, storing function expressions and column definition information.

## Definition


## Detailed Description
RangeTblFunction represents individual functions within a FUNCTION range table entry. When a query contains function calls in the FROM clause, each function gets its own RangeTblFunction structure. This structure is particularly important for handling functions that return RECORD types with explicit column definition lists.

The structure stores the function expression tree and optional column definition information. When a function has an explicit column definition list (required for RECORD-returning functions), the column names, types, type modifiers, and collations are stored in the respective list fields. For functions returning named composite types, column information is not stored since it can change over time, but the column count is preserved to handle schema evolution gracefully.

During query planning, the funcparams bitmapset is populated to track PARAM_EXEC parameters that affect the function, enabling proper parameter handling during execution.

## Parameters / Member Variables
- : NodeTag identifying this as a RangeTblFunction node
- : Expression tree representing the function call
- : Number of columns this function contributes to the RTE
- : List of column names from explicit column definition list
- : List of OIDs representing column types from definition list
- : List of type modifiers for columns from definition list
- : List of OIDs representing column collations from definition list
- : Bitmapset of PARAM_EXEC parameter IDs affecting this function (set during planning)

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag
  - Node
  - List
  - Bitmapset
- Called from (representative examples):
  - addRangeTableEntryForFunction
  - ExecInitFunctionScan
  - ExecReScanFunctionScan
  - expandRTE
  - get_from_clause_item
  - inline_set_returning_function
  - set_function_size_estimates

## Notes and Other Information
- Only the funcexpr field is included in query jumbling for performance optimization
- Column definition information is stored only when explicitly provided (e.g., for RECORD functions)
- For named composite types, column information changes are handled by preserving column count
- The funcparams field is populated during planning phase for executor use
- Multiple RangeTblFunction entries can exist within a single FUNCTION RTE
- Handles both simple function calls and complex set-returning functions with column definitions
- Critical for proper execution of functions in FROM clauses and lateral joins