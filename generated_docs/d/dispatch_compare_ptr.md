# dispatch_compare_ptr

## Location
[src/backend/executor/execExprInterp.c:2373-2389](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L2373-L2389)

## Overview
A comparison function used for sorting ExprEvalOpLookup structures when building the address-to-opcode lookup table for threaded dispatch in ExecEvalStepOp.

## Definition

```c
static int
dispatch_compare_ptr(const void *a, const void *b)
```
## Detailed Description
This function serves as a comparator for qsort() when building a lookup table that maps jump target addresses to opcodes in PostgreSQL's expression evaluation interpreter. In the threaded dispatch implementation, this lookup table is crucial for converting computed goto addresses back to their corresponding ExprEvalOp opcodes. The function performs a simple integer comparison on the opcode field of ExprEvalOpLookup structures.

## Parameters / Member Variables
- : Pointer to the first ExprEvalOpLookup structure to compare
- : Pointer to the second ExprEvalOpLookup structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - [ExprEvalOpLookup](../E/ExprEvalOpLookup.md) (struct type)
- Called from (representative examples):
  - [ExecInitInterpreter](../E/ExecInitInterpreter.md) (at line 2410)
  - [ExecEvalStepOp](../E/ExecEvalStepOp.md) (at line 2435)

## Notes and Other Information
- This is a static function only used within execExprInterp.c
- Returns -1 if first opcode is less than second, 1 if greater, 0 if equal
- Used specifically for threaded dispatch optimization in expression evaluation
- The comparison is based on the opcode field rather than the memory address

## Simplified Source

```c
static int dispatch_compare_ptr(const void *a, const void *b)
{
    const ExprEvalOpLookup *la = (const ExprEvalOpLookup *) a;
    const ExprEvalOpLookup *lb = (const ExprEvalOpLookup *) b;

    // Simple integer comparison for qsort
    if (la->opcode < lb->opcode)
        return -1;
    else if (la->opcode > lb->opcode)
        return 1;
    return 0;
}
```