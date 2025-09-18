# logicalrep_write_prepare

## Location
[src/backend/replication/logical/proto.c:198-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L198-L209)

## Overview
Writes a PREPARE message to the logical replication output stream to signal the completion of a prepared transaction.

## Definition
```c
void logicalrep_write_prepare(StringInfo out, ReorderBufferTXN *txn, XLogRecPtr prepare_lsn)
```

## Detailed Description
This function is a simple wrapper around logicalrep_write_prepare_common that specifically writes a LOGICAL_REP_MSG_PREPARE message. It represents the final step in a two-phase commit transaction within the logical replication protocol, indicating that the transaction has been prepared and is ready for commit or rollback. The function delegates all the actual serialization work to the common implementation while specifying the appropriate message type.

## Parameters / Member Variables
- `out`: StringInfo buffer where the serialized PREPARE message will be written
- `txn`: ReorderBufferTXN structure containing transaction information to be serialized
- `prepare_lsn`: XLogRecPtr specifying the LSN where the prepare operation occurred

## Dependencies
- Functions called/Symbols referenced:
  - [logicalrep_write_prepare_common](logicalrep_write_prepare_common.md)
  - LOGICAL_REP_MSG_PREPARE
  - [ReorderBufferTXN](../R/ReorderBufferTXN.md)
- Called from (representative examples):
  - [pgoutput_prepare_txn](../p/pgoutput_prepare_txn.md)

## Notes and Other Information
- Simple wrapper function that delegates to logicalrep_write_prepare_common
- Part of the two-phase commit protocol in logical replication
- Complements logicalrep_write_begin_prepare by marking the end of the prepare phase
- The prepare_lsn parameter allows specifying the exact LSN where preparation occurred
- Located in src/backend/replication/logical/proto.c:198-209