# pg_logical_emit_message_bytea

## Location
[src/backend/replication/logical/logicalfuncs.c:368-381](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logicalfuncs.c#L368-L381)

## Overview
Writes a logical decoding message with binary data into the Write-Ahead Log (WAL), allowing custom application messages to be included in the logical replication stream.

## Definition
```sql
CREATE OR REPLACE FUNCTION pg_logical_emit_message(
    transactional boolean,
    prefix text,
    message bytea,
    flush boolean DEFAULT false
) RETURNS pg_lsn
```

```c
Datum pg_logical_emit_message_bytea(PG_FUNCTION_ARGS)
```

## Detailed Description
This SQL function allows applications to write custom messages containing binary data into the WAL that will be visible to logical decoding consumers. The function writes a logical message record that includes a text prefix and binary message data. The message can be either transactional (included in a transaction and subject to rollback) or non-transactional (immediately visible regardless of transaction state).

The function is implemented by extracting the parameters and calling `LogLogicalMessage()` to write the actual WAL record. The binary data is passed through without conversion, allowing arbitrary binary content to be included in the logical replication stream.

## Parameters / Member Variables
- `transactional`: If true, the message is transactional and subject to rollback; if false, its immediately visible
- `prefix`: Text prefix to identify the message type or source
- `message`: Binary data to include in the message
- `flush`: If true and message is non-transactional, forces immediate disk write (default false)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOOL
  - text_to_cstring
  - PG_GETARG_BYTEA_PP
  - [LogLogicalMessage](../L/LogLogicalMessage.md)
  - PG_RETURN_LSN
- Called from (representative examples):
  - [pg_logical_emit_message_text](pg_logical_emit_message_text.md)
  - Direct SQL function calls from applications

## Notes and Other Information
- Returns the LSN where the message was written
- Binary variant of the logical message emission functionality
- Useful for including custom binary markers, metadata, or application-specific data in logical replication streams
- The prefix and message data are stored in WAL and will be decoded by logical replication consumers
- Complementary to pg_logical_emit_message_text which handles text messages
- Defined in src/backend/replication/logical/logicalfuncs.c:368-381