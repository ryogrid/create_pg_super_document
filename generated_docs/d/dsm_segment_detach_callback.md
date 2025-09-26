# dsm_segment_detach_callback

## Location
[src/backend/storage/ipc/dsm.c:58-63](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm.c#L58-L63)

## Overview
A backend-local structure that tracks callback functions to be executed when a dynamic shared memory (DSM) segment is detached.

## Definition

```c
typedef struct dsm_segment_detach_callback
{
	on_dsm_detach_callback function;
	Datum		arg;
	slist_node	node;
} dsm_segment_detach_callback;
```
## Detailed Description
This structure represents a single callback registration in PostgreSQL's Dynamic Shared Memory (DSM) system. When a DSM segment is detached from the current backend, all registered callbacks stored in structures of this type are executed. The structure is designed to be stored in a singly-linked list, allowing multiple callbacks to be registered per DSM segment. Each callback consists of a function pointer and an associated argument that will be passed to the function when invoked.

## Parameters / Member Variables
- : A function pointer of type  that points to the callback function to be executed on detachment
- : A  value that serves as an argument to be passed to the callback function when it is invoked
- : An  structure that allows this callback to be linked into a singly-linked list of callbacks

## Dependencies
- Functions called/Symbols referenced:
  - [slist_node](../s/slist_node.md) (for linked list functionality)
  - on_dsm_detach_callback (callback function type)
  - Datum (PostgreSQL's generic data type)
- Called from (representative examples):
  - [dsm_detach](dsm_detach.md) (uses this structure when processing callbacks)
  - [on_dsm_detach](../o/on_dsm_detach.md) (creates instances of this structure)
  - [cancel_on_dsm_detach](../c/cancel_on_dsm_detach.md) (searches for and removes instances)
  - [reset_on_dsm_detach](../r/reset_on_dsm_detach.md) (manages instances during cleanup)

## Notes and Other Information
- The callback function type is defined as: 
- This structure is part of PostgreSQL's DSM infrastructure, which provides shared memory segments that can be attached to multiple backends
- Callbacks are typically used for cleanup operations that need to occur when a segment is no longer accessible
- The slist_node allows for efficient insertion and removal of callbacks from the per-segment callback list