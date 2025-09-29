# max_parallel_hazard_test

## Location
[src/backend/optimizer/util/clauses.c:794-821](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L794-L821)

## Overview
Core logic function that evaluates the parallel hazard level of a database object and updates the maximum hazard context accordingly, serving as the central decision point for parallel safety analysis.

## Definition

```c
struct */
			return true;
```
## Detailed Description
The  function implements the core logic for all parallel-hazard checks in PostgreSQL. It takes a parallel safety classification and a context structure, then determines whether to continue traversing an expression tree or stop due to reaching an unacceptable hazard level.

The function operates on three parallel safety levels:
- : Functions that are completely safe to run in parallel workers
- : Functions that can run in parallel but with restrictions (e.g., cannot be pushed below a Gather node)
- : Functions that cannot run in parallel workers at all

The function updates the context's  field to track the highest hazard level encountered so far. It implements an early termination optimization: if the function encounters a hazard level that matches or exceeds the  threshold, it returns true to signal that traversal should stop.

For  functions, the traversal always stops immediately since no further analysis is needed - the expression is definitively not safe for parallel execution.

## Parameters / Member Variables
- : Character indicating the parallel safety level of the current database object being evaluated
- : Pointer to max_parallel_hazard_context structure that tracks the maximum hazard level and traversal state

## Dependencies
- Functions called/Symbols referenced:
  - max_parallel_hazard_context
  - PROPARALLEL_SAFE
  - PROPARALLEL_RESTRICTED
  - PROPARALLEL_UNSAFE
- Called from (representative examples):
  - [max_parallel_hazard_checker](max_parallel_hazard_checker.md)
  - [max_parallel_hazard_walker](max_parallel_hazard_walker.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the clauses.c compilation unit
- The function includes an assertion to ensure max_hazard is never UNSAFE when processing RESTRICTED items, which helps catch logic errors during development
- The early termination mechanism is crucial for performance, avoiding unnecessary traversal once an unacceptable hazard level is found
- Uses elog(ERROR) for unrecognized proparallel values, which will terminate query processing with an error message
- Located in src/backend/optimizer/util/clauses.c:794-821

## Simplified Source

```c
static bool max_parallel_hazard_test(char proparallel, max_parallel_hazard_context *context) {
    // Test parallel safety level and update context
    switch (proparallel) {
        case PROPARALLEL_SAFE:
            // Safe for parallel execution - continue checking
            break;

        case PROPARALLEL_RESTRICTED:
            // Update hazard level to restricted
            context->max_hazard = proparallel;

            // Stop if we've reached the maximum interesting level
            if (context->max_interesting == proparallel)
                return true;
            break;

        case PROPARALLEL_UNSAFE:
            // Unsafe for parallel - always stop traversal
            context->max_hazard = proparallel;
            return true;

        default:
            elog(ERROR, "unrecognized proparallel value \"%c\"", proparallel);
    }

    return false;
}
```