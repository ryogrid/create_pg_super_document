# check_stack_depth

## Location
[src/backend/tcop/postgres.c:3558-3571](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L3558-L3571)

## Overview
check_stack_depth monitors recursion depth and throws an error when the stack depth limit is exceeded, preventing stack overflow crashes in PostgreSQL.

## Definition

```c
void
check_stack_depth(void)
```
## Detailed Description
check_stack_depth is a critical safety mechanism that prevents stack overflow by monitoring the current call stack depth against a configurable limit. When the stack depth exceeds the safe threshold defined by max_stack_depth, it immediately throws an ERROR to prevent the process from crashing due to hardware stack overflow.

The function works by:
1. Calling stack_is_too_deep() to determine if the current stack depth exceeds the safe limit
2. If the limit is exceeded, throwing a STATEMENT_TOO_COMPLEX error with detailed information about the current max_stack_depth setting
3. Providing a helpful hint to increase the max_stack_depth configuration parameter if needed

This function should be called in any recursive routine that might potentially recurse deep enough to overflow the stack. Most Unix systems treat stack overflow as an unrecoverable SIGSEGV signal, so PostgreSQL proactively prevents this by checking the stack depth and gracefully erroring out before hitting the hardware limit.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [stack_is_too_deep](../s/stack_is_too_deep.md)
  - ereport (for error reporting)
  - max_stack_depth (configuration parameter)
- Called from (representative examples):
  - [gistSplit](../g/gistSplit.md)
  - [findDependentObjects](../f/findDependentObjects.md)
  - [ExecInitExprRec](../E/ExecInitExprRec.md)
  - [transformExprRecurse](../t/transformExprRecurse.md)
  - [eval_const_expressions_mutator](../e/eval_const_expressions_mutator.md)
  - [copyObjectImpl](copyObjectImpl.md)
  - [equal](../e/equal.md)
  - [outNode](../o/outNode.md)
  - Many other recursive functions throughout PostgreSQL

## Notes and Other Information
- This is part of PostgreSQL's defensive programming strategy to prevent crashes
- The function is widely used throughout the codebase in potentially recursive operations
- The max_stack_depth parameter can be tuned but should be set carefully to avoid exceeding platform limits
- For code that wants to handle the stack depth condition rather than immediately erroring, stack_is_too_deep() can be used instead
- Critical for preventing stack overflow in complex queries, deep object hierarchies, and recursive operations
- The error includes helpful guidance about adjusting the max_stack_depth configuration parameter

## Simplified Source

```c
// Simplified version of check_stack_depth
void check_stack_depth(void) {
    // Check if current stack usage exceeds safe limits
    if (stack_is_too_deep()) {
        // Throw error with helpful configuration guidance
        ereport(ERROR,
                (errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
                 errmsg("stack depth limit exceeded"),
                 errhint("Increase the configuration parameter \"max_stack_depth\" (currently %dkB), "
                         "after ensuring the platform's stack depth limit is adequate.",
                         max_stack_depth)));
    }
}
```

Key simplifications made:
- Added clear comments explaining the two main steps: checking depth and reporting error
- Preserved the essential logic flow: check condition, then error if needed
- Maintained the complete error reporting structure as it's critical for user guidance
- Simplified function comment to focus on core purpose
- No significant code reduction needed as the original function is already quite concise and focused