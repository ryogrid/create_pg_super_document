# MemoryContextMethods

## Location
src/include/nodes/memnodes.h: 58 - 114

## Overview
A function pointer structure that defines the interface for memory context implementations, providing a standardized set of operations for memory allocation, deallocation, and management across different memory context types in PostgreSQL.

## Definition
```c
typedef struct MemoryContextMethods
{
    void       *(*alloc) (MemoryContext context, Size size, int flags);
    void        (*free_p) (void *pointer);
    void       *(*realloc) (void *pointer, Size size, int flags);
    void        (*reset) (MemoryContext context);
    void        (*delete_context) (MemoryContext context);
    MemoryContext (*get_chunk_context) (void *pointer);
    Size        (*get_chunk_space) (void *pointer);
    bool        (*is_empty) (MemoryContext context);
    void        (*stats) (MemoryContext context,
                          MemoryStatsPrintFunc printfunc, void *passthru,
                          MemoryContextCounters *totals,
                          bool print_to_stderr);
#ifdef MEMORY_CONTEXT_CHECKING
    void        (*check) (MemoryContext context);
#endif
} MemoryContextMethods;
```

## Detailed Description
MemoryContextMethods serves as the virtual function table for PostgreSQL's memory context system, implementing a polymorphic interface that allows different memory allocation strategies to be used interchangeably. Each memory context type (AllocSet, Generation, Bump, Slab) provides its own implementation of these methods, enabling specialized allocation patterns while maintaining a uniform API.

The structure supports various allocation flags including MCXT_ALLOC_HUGE for large allocations, MCXT_ALLOC_NO_OOM for non-throwing allocations, and MCXT_ALLOC_ZERO for zero-initialized memory. The methods handle both the basic allocation operations and advanced features like statistics collection and memory validation.

## Parameters / Member Variables
- `alloc`: Function to allocate memory of specified size with given flags in the context
- `free_p`: Function to deallocate a previously allocated pointer (named free_p to avoid conflicts with free() macro)
- `realloc`: Function to resize an existing allocation, handling size changes and flags
- `reset`: Function to invalidate all allocations and prepare context for reuse, optionally returning excess memory to OS
- `delete_context`: Function to completely free all memory consumed by the context
- `get_chunk_context`: Function to determine which memory context owns a given pointer
- `get_chunk_space`: Function to return total bytes consumed by a pointer including overhead
- `is_empty`: Function to check if context has no allocations since creation or last reset
- `stats`: Function to collect and report memory usage statistics using MemoryContextCounters
- `check`: Function to perform validation checks (only available when MEMORY_CONTEXT_CHECKING is defined)

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextCounters](MemoryContextCounters.md)
  - [MemoryContext](MemoryContext.md)
  - MemoryStatsPrintFunc
  - Size
  - MEMORY_CONTEXT_CHECKING (conditional compilation)
- Called from (representative examples):
  - [MemoryContextData](MemoryContextData.md) (memnodes.h:126)
  - BOGUS_MCTX (mcxt.c:46)

## Notes and Other Information
- Acts as a virtual function table enabling polymorphism in C
- Each memory context implementation (AllocSet, Generation, Bump, Slab) provides its own method implementations
- The alloc and realloc methods must handle MCXT_ALLOC_HUGE and MCXT_ALLOC_NO_OOM flags
- MCXT_ALLOC_ZERO flag is handled by calling functions, not by the methods themselves
- The check method is only available in debug builds when MEMORY_CONTEXT_CHECKING is enabled
- Located in src/include/nodes/memnodes.h as part of the core memory management interface
- Enables PostgreSQL to support different allocation strategies while maintaining a consistent API