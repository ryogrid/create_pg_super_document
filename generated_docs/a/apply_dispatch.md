# apply_dispatch

## Location
[src/backend/replication/logical/worker.c:3285-3404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L3285-L3404)

## Overview
Central message dispatcher for logical replication protocol messages, routing incoming messages to appropriate handler functions based on message type.

## Definition

```c
void
apply_dispatch(StringInfo s)
```
## Detailed Description
This function serves as the main message dispatcher for PostgreSQL's logical replication worker. It implements a comprehensive switch-based routing system that:

1. **Message Type Extraction**: Reads the first byte from the message buffer to determine the logical replication message type
2. **Error Context Management**: Maintains error callback context by saving and restoring the current command being processed, supporting recursive calls during spooled message processing  
3. **Message Routing**: Dispatches to specialized handler functions based on message type including:
   - **Transaction Control**: BEGIN, COMMIT, PREPARE, COMMIT_PREPARED, ROLLBACK_PREPARED
   - **DML Operations**: INSERT, UPDATE, DELETE, TRUNCATE
   - **Schema Changes**: RELATION (relation definition), TYPE (type definition)  
   - **Streaming Support**: STREAM_START, STREAM_STOP, STREAM_ABORT, STREAM_COMMIT, STREAM_PREPARE
   - **Replication Control**: ORIGIN, BEGIN_PREPARE
   - **Generic Messages**: MESSAGE (currently unused but reserved for extensions)

4. **Error Handling**: Provides comprehensive error reporting for invalid or unrecognized message types with protocol violation errors

The function is designed to be recursively callable, which is essential when processing spooled messages that may contain nested message sequences.

## Parameters / Member Variables
- `s`: StringInfo buffer containing the incoming logical replication message with the message type as the first byte followed by message-specific data
## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgbyte](../p/pq_getmsgbyte.md)
  - [apply_handle_begin](apply_handle_begin.md)
  - [apply_handle_commit](apply_handle_commit.md)  
  - [apply_handle_insert](apply_handle_insert.md)
  - [apply_handle_update](apply_handle_update.md)
  - [apply_handle_delete](apply_handle_delete.md)
  - [apply_handle_truncate](apply_handle_truncate.md)
  - [apply_handle_relation](apply_handle_relation.md)
  - [apply_handle_type](apply_handle_type.md)
  - [apply_handle_origin](apply_handle_origin.md)
  - [apply_handle_stream_start](apply_handle_stream_start.md)
  - [apply_handle_stream_stop](apply_handle_stream_stop.md)
  - [apply_handle_stream_abort](apply_handle_stream_abort.md)
  - [apply_handle_stream_commit](apply_handle_stream_commit.md)
  - [apply_handle_begin_prepare](apply_handle_begin_prepare.md)
  - [apply_handle_prepare](apply_handle_prepare.md)
  - [apply_handle_commit_prepared](apply_handle_commit_prepared.md)
  - [apply_handle_rollback_prepared](apply_handle_rollback_prepared.md)
  - [apply_handle_stream_prepare](apply_handle_stream_prepare.md)
- Called from (representative examples):
  - [LogicalParallelApplyLoop](../L/LogicalParallelApplyLoop.md)
  - [apply_spooled_messages](apply_spooled_messages.md)  
  - [LogicalRepApplyLoop](../L/LogicalRepApplyLoop.md)

## Notes and Other Information
- Supports recursive invocation for processing spooled messages during streaming transactions
- The LOGICAL_REP_MSG_MESSAGE case is currently unused but reserved for potential extension use
- Maintains proper error callback context to provide meaningful error messages during message processing
- All message handlers are expected to fully consume their respective message data from the StringInfo buffer
- Protocol violation errors are raised for unrecognized message types to maintain strict protocol compliance