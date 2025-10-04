# MemoryContextSwitchTo

## Location
[src/include/utils/palloc.h:124-151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/palloc.h#L124-L151)

## Overview
A static inline function that switches the current memory context to a specified context and returns the previously active memory context.

## Definition

```c
static inline MemoryContext
MemoryContextSwitchTo(MemoryContext context)
```
## Detailed Description
MemoryContextSwitchTo is a fundamental memory management utility in PostgreSQL that changes the current memory allocation context. It atomically switches the global CurrentMemoryContext variable to the specified context and returns the previously active context. This function is essential for PostgreSQL's memory management system, allowing code to temporarily switch to a different memory context for allocations and then restore the previous context. The function is implemented as a static inline for performance reasons since it's frequently called throughout the PostgreSQL codebase.

## Parameters / Member Variables
- `context`: The MemoryContext to switch to as the new current memory context
## Dependencies
- Functions called/Symbols referenced:
  - CurrentMemoryContext (global variable)
  - [MemoryContext](MemoryContext.md) (type definition)
- Called from (representative examples):
  - Various test modules and PL/Python extensions
  - Memory allocation routines throughout PostgreSQL

## Notes and Other Information
- Implemented as a static inline function for optimal performance
- Returns the previous memory context, enabling easy restoration patterns
- Critical for PostgreSQL's memory management architecture
- Typically used in conjunction with memory allocation functions
- Located in src/include/utils/palloc.h:124-151
- Common usage pattern: save old context, switch to new context, perform operations, restore old context

## Simplified Source

```c
static inline MemoryContext
MemoryContextSwitchTo(MemoryContext context)
{
    MemoryContext old = CurrentMemoryContext;

    CurrentMemoryContext = context;
    return old;
}
```