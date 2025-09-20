# LogLogicalMessage

## Location
[src/backend/replication/logical/message.c:43-86](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/message.c#L43-L86)

## Overview
LogLogicalMessage writes logical decoding messages into the transaction log (XLog) and is used for emitting custom messages that can be consumed by logical replication subscribers.

## Definition

```c
XLogRecPtr
LogLogicalMessage(const char *prefix, const char *message, size_t size,
				  bool transactional, bool flush)
```
## Detailed Description
This function creates and logs a logical message record in the write-ahead log (WAL). It supports both transactional and non-transactional messages. Transactional messages are included in the transaction's commit record and are replayed only if the transaction commits, while non-transactional messages are immediately visible to logical decoding plugins regardless of transaction state. The function constructs an xl_logical_message record containing the database ID, message metadata, prefix, and message content, then inserts it into the XLog using the XLOG_LOGICAL_MESSAGE operation code.

## Parameters / Member Variables
- `prefix`: A null-terminated string that serves as a message identifier or category
- `message`: The actual message content (can contain binary data)
- `size`: The size of the message content in bytes
- `transactional`: If true, the message is tied to the current transaction; if false, it's immediately visible
- `flush`: If true and the message is non-transactional, forces the WAL record to be flushed to disk before returning

## Dependencies
- Functions called/Symbols referenced:
  - [xl_logical_message](../x/xl_logical_message.md)
  - [IsTransactionState](../I/IsTransactionState.md)
  - [GetCurrentTransactionId](../G/GetCurrentTransactionId.md)
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - SizeOfLogicalMessage
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - unconstify
  - [XLogSetRecordFlags](../X/XLogSetRecordFlags.md)
  - XLOG_INCLUDE_ORIGIN
  - [XLogInsert](../X/XLogInsert.md)
  - XLOG_LOGICAL_MESSAGE
  - [XLogFlush](../X/XLogFlush.md)
- Called from (representative examples):
  - [pg_logical_emit_message_bytea](../p/pg_logical_emit_message_bytea.md)

## Notes and Other Information
- For transactional messages, a transaction ID is forcibly allocated to ensure proper transaction tracking
- The prefix string must include a trailing zero byte, which is critical for proper message description
- Non-transactional messages with flush=true are immediately written to disk for durability
- The function sets XLOG_INCLUDE_ORIGIN flag to allow origin filtering in logical replication
- Returns the LSN (Log Sequence Number) of the inserted WAL record