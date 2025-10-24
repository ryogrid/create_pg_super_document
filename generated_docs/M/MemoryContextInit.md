# MemoryContextInit

## Location
[src/backend/utils/mmgr/mcxt.c:339-382](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L339-L382)

## Overview
MemoryContextInit initializes PostgreSQL's memory context subsystem, setting up the fundamental TopMemoryContext and ErrorContext that serve as the foundation for all memory management operations.

## Definition
```c
void MemoryContextInit(void)
```

## Detailed Description
This function is a critical initialization routine that establishes PostgreSQL's memory management infrastructure. It must be called before any memory contexts can be created or memory allocated within contexts. The function sets up two essential contexts:

1. **TopMemoryContext**: The root parent context for all other memory contexts, created using default AllocSet parameters
2. **ErrorContext**: A specialized context designed for error handling with conservative memory usage (8KB minimum retained) and the ability to allocate during critical sections

The function includes important safeguards and design considerations:
- Asserts that TopMemoryContext is NULL to prevent double initialization
- Sets CurrentMemoryContext to TopMemoryContext as a temporary measure
- Configures ErrorContext with slow growth rate and guaranteed minimum memory
- Allows ErrorContext allocations during critical sections to ensure error reporting works even under memory pressure

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate (to create both TopMemoryContext and ErrorContext)
  - ALLOCSET_DEFAULT_SIZES (default sizing parameters for TopMemoryContext)
  - [MemoryContextAllowInCriticalSection](MemoryContextAllowInCriticalSection.md) (to enable ErrorContext allocations during critical sections)
- Called from (representative examples):
  - [main](../m/main.md) (during PostgreSQL startup in src/backend/main/main.c)
  - AllocHugeSizeIsValid (referenced in header file)

## Notes and Other Information
- Must be called exactly once during process startup before any memory context operations
- In multi-backend operation, called once during postmaster startup; backends inherit initialized contexts
- In EXEC_BACKEND builds, each process must call this function independently
- Standalone backends must call this during startup
- ErrorContext initialization is the final step because elog.c depends on ErrorContext being non-null
- The 8KB minimum for ErrorContext ensures error reporting works even under severe memory pressure
- TopMemoryContext serves as the ultimate parent for PostgreSQL's memory context hierarchy
- CurrentMemoryContext assignment is temporary - callers should update it appropriately for their needs

## Simplified Source

```c
void
MemoryContextInit(void)
{
    Assert(TopMemoryContext == NULL);

    // Create the root memory context
    TopMemoryContext = AllocSetContextCreate((MemoryContext) NULL,
                                             "TopMemoryContext",
                                             ALLOCSET_DEFAULT_SIZES);

    // Set current context to top context (temporary)
    CurrentMemoryContext = TopMemoryContext;

    // Create error context with guaranteed 8KB minimum and slow growth
    ErrorContext = AllocSetContextCreate(TopMemoryContext,
                                        "ErrorContext",
                                        8 * 1024,    // initBlockSize
                                        8 * 1024,    // minContextSize
                                        8 * 1024);   // maxBlockSize

    // Allow ErrorContext to allocate during critical sections
    MemoryContextAllowInCriticalSection(ErrorContext, true);
}
```