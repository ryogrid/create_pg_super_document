# MemoryContextAllocZero

## Location
src/backend/utils/mmgr/mcxt.c: 1214 - 1236

## Overview
MemoryContextAllocZero allocates zero-initialized memory within a specified PostgreSQL memory context, combining allocation and memory clearing in a single optimized operation.

## Definition
```c
void *MemoryContextAllocZero(MemoryContext context, Size size)
```

## Detailed Description
This function provides a convenient and efficient way to allocate memory that is guaranteed to be initialized to zero. While the same result could be achieved by calling MemoryContextAlloc followed by memory clearing, this function combines both operations for better performance since zero-initialized allocation is a very common pattern in PostgreSQL.

The function follows the same validation and context management patterns as MemoryContextAlloc, performing context validation, critical section checks, and context state updates. After allocating memory through the context's allocation method, it uses the optimized MemSetAligned function to clear the allocated memory to zero.

Like MemoryContextAlloc, this function integrates with Valgrind memory debugging tools and delegates allocation failure handling to the context-specific methods for optimal performance.

## Parameters / Member Variables
- `context`: The memory context in which to allocate zero-initialized memory
- `size`: The number of bytes to allocate and zero-initialize

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextIsValid (validates the context)
  - AssertNotInCriticalSection (ensures safe allocation timing) 
  - VALGRIND_MEMPOOL_ALLOC (integrates with Valgrind debugging)
  - MemSetAligned (efficiently clears memory to zero)
- Called from (representative examples):
  - [index_form_tuple_context](../i/index_form_tuple_context.md)
  - [InitializeSession](../I/InitializeSession.md)
  - [PushTransaction](../P/PushTransaction.md)
  - CreatePortal
  - ResourceOwnerCreate
  - array manipulation functions
  - relation cache operations
  - replication buffer operations

## Notes and Other Information
- This function is a performance optimization for the common pattern of allocating and then clearing memory
- MemSetAligned is used for efficient memory clearing, taking advantage of system-specific optimizations
- The function follows the same critical section restrictions as MemoryContextAlloc
- Context state management (isReset = false) is identical to MemoryContextAlloc
- Valgrind integration tracks the allocation but not the subsequent clearing operation
- This function is widely used throughout PostgreSQL for initializing data structures that require clean memory
- The combination of allocation and clearing in one function reduces function call overhead
- Like MemoryContextAlloc, this function serves as a foundation for higher-level functions like palloc0()
- Memory clearing happens after allocation, so allocation failures are handled before any clearing occurs