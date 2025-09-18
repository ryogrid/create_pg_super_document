# pq_sendbyte

## Location
src/include/libpq/pqformat.h: 160 - 170

## Overview
Appends a single binary byte to a StringInfo buffer for network transmission in PostgreSQL protocol messages.

## Definition
```c
static inline void pq_sendbyte(StringInfo buf, uint8 byt)
```

## Detailed Description
`pq_sendbyte` is a convenient inline function that appends a single byte to a StringInfo buffer. It serves as a semantic wrapper around `pq_sendint8`, providing clearer intent when sending individual bytes rather than 8-bit integers. This function is extensively used throughout PostgreSQL for protocol message type indicators, flags, boolean values, and other single-byte data elements. It is particularly prominent in logical replication protocol messages, error message formatting, and data type serialization where individual bytes serve as message markers or state indicators.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the byte will be appended
- `byt`: The 8-bit unsigned integer (byte) value to append to the buffer

## Dependencies
- Functions called/Symbols referenced:
  - pq_sendint8
- Called from (representative examples):
  - logicalrep_write_begin
  - logicalrep_write_commit
  - logicalrep_write_insert
  - logicalrep_write_update
  - logicalrep_write_delete
  - send_feedback
  - XLogWalRcvSendReply
  - WalSndPrepareWrite
  - send_message_to_frontend
  - boolsend
  - charsend
  - macaddr_send
  - network_send

## Notes and Other Information
- Defined as a static inline function for optimal performance
- Semantic wrapper around pq_sendint8 for improved code readability
- Heavily used in logical replication for message type indicators
- Essential for PostgreSQL protocol message formatting
- Used extensively in error message construction and frontend communication
- Common for sending boolean values, flags, and enumeration constants
- Provides clear intent when sending single bytes vs. 8-bit numeric values
- Critical component of PostgreSQL's wire protocol implementation