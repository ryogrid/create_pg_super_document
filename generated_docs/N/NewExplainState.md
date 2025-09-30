# NewExplainState

## Location
[src/backend/commands/explain.c:372-388](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L372-L388)

## Overview
NewExplainState creates and initializes a new ExplainState structure with default options for EXPLAIN command processing.

## Definition

```c
ExplainState *
NewExplainState(void)
```
## Detailed Description
NewExplainState is a constructor function that allocates and initializes an ExplainState structure. It uses palloc0 to zero-initialize all fields, then sets the default values for EXPLAIN options. By default, only the costs option is enabled (set to true), while other options like analyze, verbose, buffers, etc., remain false. The function also initializes the output string buffer using makeStringInfo(), which creates a dynamically expandable StringInfo structure for accumulating the explain output.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [ExplainState](../E/ExplainState.md) (struct type)
  - [palloc0](../p/palloc0.md)
  - [makeStringInfo](../m/makeStringInfo.md)
- Called from (representative examples):
  - [ExplainQuery](../E/ExplainQuery.md)

## Notes and Other Information
- Uses palloc0 for zero-initialization, ensuring all boolean flags start as false
- Only the 'costs' option is enabled by default, reflecting PostgreSQL's standard EXPLAIN behavior
- The StringInfo buffer is pre-allocated to handle output accumulation efficiently
- Memory is allocated in the current memory context (typically the query execution context)

## Simplified Source

```c
ExplainState *NewExplainState(void) {
    // Allocate and zero-initialize ExplainState structure
    ExplainState *es = (ExplainState *) palloc0(sizeof(ExplainState));

    // Set default options - costs enabled by default
    es->costs = true;

    // Initialize output buffer for explain text
    es->str = makeStringInfo();

    return es;
}
```