# ExecInitInterpreter

## Location
src/backend/executor/execExprInterp.c: 2390 - 2421

## Overview
One-time initialization function for PostgreSQL's expression evaluation interpreter that sets up dispatch tables for computed goto optimization.

## Definition
```c
static void ExecInitInterpreter(void)
```

## Detailed Description
This function performs crucial one-time setup for the expression evaluation interpreter when compiled with computed goto support (EEO_USE_COMPUTED_GOTO). It initializes the dispatch table by calling ExecInterpExpr with NULL parameters to obtain jump target addresses, then builds a reverse lookup table that maps these addresses back to their corresponding ExprEvalOp opcodes. The reverse lookup table is sorted using qsort with dispatch_compare_ptr as the comparator to enable efficient binary search operations later.

## Parameters / Member Variables
- None (void function with no parameters)

## Dependencies
- Functions called/Symbols referenced:
  - ExecInterpExpr (to obtain dispatch table addresses)
  - qsort (to sort the reverse lookup table)
  - dispatch_compare_ptr (comparator function for sorting)
  - EEOP_LAST (constant defining number of opcodes)
  - ExprEvalOp (enumeration type)
  - ExprEvalOpLookup (structure type)
- Called from (representative examples):
  - EEO_JUMP macro (at line 145)
  - ExecReadyInterpretedExpr (at line 239)

## Notes and Other Information
- Only compiled when EEO_USE_COMPUTED_GOTO is defined
- Static function visible only within execExprInterp.c
- Uses global variables dispatch_table and reverse_dispatch_table
- The reverse lookup table enables mapping from jump addresses back to opcodes
- Critical for threaded dispatch optimization in expression evaluation
- Initialization is performed only once when dispatch_table is NULL