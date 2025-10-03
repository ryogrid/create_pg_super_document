# ExecInitInterpreter

## Location
[src/backend/executor/execExprInterp.c:2390-2421](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L2390-L2421)

## Overview
One-time initialization function for PostgreSQL's expression evaluation interpreter that sets up dispatch tables for computed goto optimization.

## Definition
```c
static void ExecInitInterpreter(void)
```

## Detailed Description
This function performs crucial one-time setup for the expression evaluation interpreter when compiled with computed goto support (EEO_USE_COMPUTED_GOTO). It initializes the dispatch table by calling ExecInterpExpr with NULL parameters to obtain jump target addresses, then builds a reverse lookup table that maps these addresses back to their corresponding ExprEvalOp opcodes. The reverse lookup table is sorted using qsort with dispatch_compare_ptr as the comparator to enable efficient binary search operations later.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [ExecInterpExpr](ExecInterpExpr.md) (to obtain dispatch table addresses)
  - qsort (to sort the reverse lookup table)
  - [dispatch_compare_ptr](../d/dispatch_compare_ptr.md) (comparator function for sorting)
  - EEOP_LAST (constant defining number of opcodes)
  - [ExprEvalOp](ExprEvalOp.md) (enumeration type)
  - [ExprEvalOpLookup](ExprEvalOpLookup.md) (structure type)
- Called from (representative examples):
  - EEO_JUMP macro (at line 145)
  - [ExecReadyInterpretedExpr](ExecReadyInterpretedExpr.md) (at line 239)

## Notes and Other Information
- Only compiled when EEO_USE_COMPUTED_GOTO is defined
- Static function visible only within execExprInterp.c
- Uses global variables dispatch_table and reverse_dispatch_table
- The reverse lookup table enables mapping from jump addresses back to opcodes
- Critical for threaded dispatch optimization in expression evaluation
- Initialization is performed only once when dispatch_table is NULL

## Simplified Source

```c
// Simplified version of ExecInitInterpreter
static void ExecInitInterpreter(void) {
#if defined(EEO_USE_COMPUTED_GOTO)
    // Initialize dispatch table only once
    if (dispatch_table == NULL) {
        // Get dispatch table addresses from interpreter
        dispatch_table = (const void **) DatumGetPointer(ExecInterpExpr(NULL, NULL, NULL));

        // Build reverse lookup table for address-to-opcode mapping
        for (int i = 0; i < EEOP_LAST; i++) {
            reverse_dispatch_table[i].opcode = dispatch_table[i];
            reverse_dispatch_table[i].op = (ExprEvalOp) i;
        }

        // Sort reverse lookup table for binary search
        qsort(reverse_dispatch_table, EEOP_LAST,
              sizeof(ExprEvalOpLookup), dispatch_compare_ptr);
    }
#endif
}
```

Key simplifications made:
- Added clear comments explaining the computed goto optimization setup
- Clarified the purpose of the reverse lookup table construction
- Maintained the essential initialization logic for expression evaluation performance