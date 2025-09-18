# apply_handle_type

## Location
[src/backend/replication/logical/worker.c:2326-2340](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L2326-L2340)

## Overview
Handles TYPE messages in PostgreSQL logical replication by reading and discarding the message data, expecting the user to have configured compatible data types between publisher and subscriber.

## Definition


## Detailed Description
This function is a message handler in the logical replication worker process that processes TYPE messages from the publisher. The implementation deliberately ignores the type information contained in these messages, operating under the assumption that the user has properly configured the subscriber's table schemas to be compatible with the incoming data types. The function simply reads the type data from the message buffer and discards it, relying on the input functions of locally subscribed tables to handle any necessary type conversions.

This approach simplifies the replication process by avoiding complex type mapping and conversion logic at the message handling level, instead delegating type compatibility concerns to the table-level input processing.

## Parameters / Member Variables
- : StringInfo buffer containing the TYPE message data to be processed

## Dependencies
- Functions called/Symbols referenced:
  - [handle_streamed_transaction](../h/handle_streamed_transaction.md)
  - [logicalrep_read_typ](../l/logicalrep_read_typ.md)
  - [LogicalRepTyp](../L/LogicalRepTyp.md) (data structure)
  - LOGICAL_REP_MSG_TYPE (constant)
- Called from (representative examples):
  - [apply_dispatch](apply_dispatch.md)

## Notes and Other Information
- This is a static function within the logical replication worker module
- The function intentionally discards type information, placing responsibility for type compatibility on the database administrator
- Part of the logical replication message dispatch system in PostgreSQL
- Handles streamed transactions by checking if the message is part of a streaming transaction before processing