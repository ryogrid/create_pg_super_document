# MemoryContextCallback

## Location
src/include/utils/palloc.h: 47 - 52

## Overview
A structure that defines callback functions to be executed just before a memory context is reset or deleted, enabling cleanup operations and resource management.

## Definition
```c
typedef struct MemoryContextCallback
{
    MemoryContextCallbackFunction func; /* function to call */
    void       *arg;                    /* argument to pass it */
    struct MemoryContextCallback *next; /* next in list of callbacks */
} MemoryContextCallback;
```

Where MemoryContextCallbackFunction is defined as:
```c
typedef void (*MemoryContextCallbackFunction) (void *arg);
```

## Detailed Description
MemoryContextCallback is a structure that implements a callback mechanism for memory contexts in PostgreSQL. It allows registering functions that will be automatically executed just before a memory context is reset or deleted. This mechanism is crucial for proper resource cleanup, allowing code to perform necessary cleanup operations (like closing files, releasing locks, or notifying other subsystems) before memory is freed. The callbacks are organized as a linked list, allowing multiple callbacks to be registered on a single context. The callback structure is typically allocated within the context itself, ensuring that the callback registration is automatically cleaned up when the context is destroyed.

## Parameters / Member Variables
- `func`: Pointer to the callback function to be executed (type MemoryContextCallbackFunction)
- `arg`: Void pointer argument that will be passed to the callback function when invoked
- `next`: Pointer to the next MemoryContextCallback in the linked list of callbacks

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextCallbackFunction (function pointer type)
  - Self-referential (next pointer to same struct type)
- Called from (representative examples):
  - [MemoryContextRegisterResetCallback](MemoryContextRegisterResetCallback.md) (callback registration)
  - MemoryContextCallResetCallbacks (callback execution)
  - [pgoutput_startup](../p/pgoutput_startup.md) (replication system usage)
  - Various subsystems for cleanup operations

## Notes and Other Information
- Defined in src/include/utils/palloc.h:47-52
- Part of PostgreSQL's memory management cleanup system
- Callbacks are executed in LIFO (Last In, First Out) order
- Typically allocated within the memory context being monitored
- Used throughout PostgreSQL for resource cleanup in replication, type caching, and plugin systems
- The callback function signature takes a single void* argument for flexibility
- Forms a singly-linked list structure for managing multiple callbacks per context
- Critical for preventing resource leaks when memory contexts are destroyed