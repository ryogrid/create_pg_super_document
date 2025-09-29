# ExecPrepareCheck

## Location
[src/backend/executor/execExpr.c:791-813](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExpr.c#L791-L813)

## Overview
Initializes check constraint expressions for execution outside a normal Plan tree context by applying planning transformations and creating an executable ExprState specialized for constraint validation.

## Definition

```c
ExprState *
ExecPrepareCheck(List *qual, EState *estate)
```
## Detailed Description
ExecPrepareCheck is a specialized function that prepares check constraint expressions for execution outside the normal query planning and execution pipeline. It is specifically designed for constraint validation expressions that need to be evaluated to ensure data integrity.

The function follows the same pattern as ExecPrepareExpr and ExecPrepareQual but is specialized for check constraints, performing:
1. **Expression planning**: Applies expression_planner() to the constraint expression list to perform optimizations and transformations suited for constraint evaluation
2. **Check constraint compilation**: Creates an executable ExprState using ExecInitCheck() that is optimized for constraint validation scenarios

Check constraints have specific evaluation semantics (must return boolean, handle NULLs appropriately for constraint logic) that ExecInitCheck is designed to handle correctly.

## Parameters / Member Variables
- : List of expressions representing the check constraint conditions (boolean expressions with constraint semantics)
- : The execution state providing the execution environment and memory context

## Dependencies
- Functions called/Symbols referenced:
  - [expression_planner](../e/expression_planner.md) (applies planning transformations to constraint expressions)
  - [ExecInitCheck](ExecInitCheck.md) (compiles check constraint into specialized ExprState)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (memory context management)
- Called from (representative examples):
  - [ExecPartitionCheck](ExecPartitionCheck.md) (partition constraint validation)

## Notes and Other Information
- **Constraint specialization**: Specifically designed for check constraint expressions with proper constraint evaluation semantics
- **NULL handling**: Check constraints have specific rules for NULL value handling that differ from regular boolean expressions
- **Standalone execution**: Enables check constraint evaluation outside the normal Plan tree execution context
- **Planning optimization**: Ensures constraint expressions receive appropriate optimizations for efficient validation
- **Memory management**: Handles memory context switching to ensure constraint expressions persist appropriately
- **Data integrity focus**: Optimized for constraint validation scenarios rather than general boolean evaluation
- **Limited usage**: Currently primarily used for partition constraint checking, indicating its specialized nature
- **Constraint semantics**: Unlike general qualifiers, check constraints must handle the three-valued logic of SQL constraints (true/false/null) correctly

## Simplified Source

```c
ExprState *
ExecPrepareCheck(List *qual, EState *estate)
{
    ExprState *result;
    MemoryContext oldcontext;

    // Switch to per-query context for persistent allocation
    oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

    // Apply planning transformations to the constraint expressions
    // This optimizes the expressions for constraint evaluation
    qual = (List *) expression_planner((Expr *) qual);

    // Compile the constraint using CHECK constraint semantics
    // (NULL results are treated as TRUE - constraint passes)
    result = ExecInitCheck(qual, NULL);

    // Restore original memory context
    MemoryContextSwitchTo(oldcontext);

    return result;
}
```