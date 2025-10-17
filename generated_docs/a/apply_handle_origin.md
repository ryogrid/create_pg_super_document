# apply_handle_origin

## Location
[src/backend/replication/logical/worker.c:1410-1430](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L1410-L1430)

## Overview
Handles ORIGIN messages in PostgreSQL logical replication to validate message ordering and maintain protocol compliance for origin tracking.

## Definition
static void apply_handle_origin(StringInfo s)

## Detailed Description
apply_handle_origin processes ORIGIN messages received from the publisher during logical replication. Currently, this function serves primarily as a protocol validator that ensures ORIGIN messages are received at appropriate times in the replication stream. The function enforces strict ordering requirements: ORIGIN messages can only be received inside streaming transactions or inside remote transactions before any actual writes occur.

The function includes a TODO comment indicating that future versions may support tracking of multiple origins, but the current implementation focuses on protocol validation. It performs state checks to ensure the replication protocol is being followed correctly and raises protocol violation errors when ORIGIN messages arrive out of order.

## Parameters / Member Variables
- : StringInfo containing the serialized ORIGIN message data from the publisher (currently unused in the implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [IsTransactionState](../I/IsTransactionState.md)
  - [am_tablesync_worker](am_tablesync_worker.md)
- Called from:
  - [apply_dispatch](apply_dispatch.md)

## Notes and Other Information
- This function currently serves primarily for protocol validation rather than functional origin tracking
- Contains a TODO indicating future support for multiple origin tracking
- Enforces strict message ordering: ORIGIN messages must come inside streaming or remote transactions
- Additional constraint: in remote transactions, ORIGIN must come before any actual database writes
- Tablesync workers have different validation rules for transaction state checking
- The function raises ERRCODE_PROTOCOL_VIOLATION errors for out-of-order messages
- The StringInfo parameter is currently not processed, suggesting the actual origin tracking implementation is pending
- Part of PostgreSQL's logical replication protocol compliance enforcement

## Simplified Source

```c
static void
apply_handle_origin(StringInfo s)
{
    // Validate ORIGIN message ordering:
    // - Must be inside streaming transaction OR
    // - Must be inside remote transaction before any writes
    if (!in_streamed_transaction &&
        (!in_remote_transaction ||
         (IsTransactionState() && !am_tablesync_worker())))
        ereport(ERROR,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                 errmsg_internal("ORIGIN message sent out of order")));

    // TODO: Future implementation will support tracking of multiple origins
    // Currently only validates protocol ordering
}
```