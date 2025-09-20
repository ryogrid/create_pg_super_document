# walrcv_clear_result

## Location
[src/include/replication/walreceiver.h:468-504](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/walreceiver.h#L468-L504)

## Overview
Cleans up and deallocates a WalRcvExecResult structure, freeing all associated memory and resources.

## Definition

```c
static inline void
walrcv_clear_result(WalRcvExecResult *walres)
```
## Detailed Description
This inline function performs complete cleanup of a WalRcvExecResult structure returned by WAL receiver execution functions. It safely deallocates all dynamically allocated components of the result structure, including error messages, tuple stores, tuple descriptors, and finally the structure itself. The function includes null-pointer checks to ensure safe operation even when called with NULL or partially initialized structures.

The function follows PostgreSQL's memory management conventions by using the appropriate deallocation functions for each component type: pfree() for general allocations, tuplestore_end() for tuple stores, and FreeTupleDesc() for tuple descriptors.

## Parameters / Member Variables
- : Pointer to the WalRcvExecResult structure to be deallocated. Can be NULL, in which case the function returns immediately without performing any operations.

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md)
  - [tuplestore_end](../t/tuplestore_end.md)
  - [FreeTupleDesc](../F/FreeTupleDesc.md)
  - [WalRcvExecResult](../W/WalRcvExecResult.md) (structure type)
- Called from (representative examples):
  - [check_publications](../c/check_publications.md) (src/backend/commands/subscriptioncmds.c:528)
  - [ReplicationSlotDropAtPubNode](../R/ReplicationSlotDropAtPubNode.md) (src/backend/commands/subscriptioncmds.c:1886)
  - [fetch_table_list](../f/fetch_table_list.md) (src/backend/commands/subscriptioncmds.c:2237)
  - [fetch_remote_table_info](../f/fetch_remote_table_info.md) (src/backend/replication/logical/tablesync.c:871)
  - [copy_table](../c/copy_table.md) (src/backend/replication/logical/tablesync.c:1258)

## Notes and Other Information
- This is an inline function defined in the header file src/include/replication/walreceiver.h
- The function performs defensive programming by checking for NULL pointers before attempting to free resources
- It properly handles the hierarchical cleanup of complex structures, ensuring no memory leaks occur
- The function is commonly used in logical replication operations, particularly in subscription management and table synchronization contexts
- All components of WalRcvExecResult are optional (can be NULL), and the function handles partial initialization gracefully