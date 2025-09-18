# logicalrep_write_message

## Location
src/backend/replication/logical/proto.c: 643 - 669

## Overview
Serializes and writes a MESSAGE to the logical replication output stream, allowing custom application messages to be transmitted to subscribers.

## Definition
```c
void logicalrep_write_message(StringInfo out, TransactionId xid, XLogRecPtr lsn,
                             bool transactional, const char *prefix, Size sz,
                             const char *message)
```

## Detailed Description
This function encodes a custom MESSAGE operation into the logical replication protocol format. It writes the message type identifier, transaction flags, transaction ID (if valid), LSN (Log Sequence Number), message prefix, size, and the actual message content. This allows applications to send custom messages through the logical replication stream that can be processed by subscribers using custom logic.

## Parameters / Member Variables
- `out`: StringInfo buffer where the serialized message will be written
- `xid`: Transaction ID associated with the message (only sent if valid)
- `lsn`: Log Sequence Number indicating the position in the WAL where this message was logged
- `transactional`: Boolean flag indicating whether this message is part of a transaction
- `prefix`: String prefix to categorize or identify the type of message
- `sz`: Size in bytes of the message content
- `message`: The actual message content to be transmitted

## Dependencies
- Functions called/Symbols referenced:
  - pq_sendbyte
  - pq_sendint32
  - pq_sendint8
  - pq_sendint64
  - pq_sendstring
  - pq_sendbytes
  - LOGICAL_REP_MSG_MESSAGE
  - MESSAGE_TRANSACTIONAL
- Called from (representative examples):
  - pgoutput_message

## Notes and Other Information
- Part of the logical replication protocol for custom application messaging
- Supports both transactional and non-transactional messages
- The prefix parameter allows message categorization for subscriber filtering
- Transaction ID is conditionally sent only when valid (for streaming transactions)
- LSN provides ordering and replication position information
- Custom messages can be used for application-specific replication needs beyond standard DML operations