# json_send

## Location
[src/backend/utils/adt/json.c:136-149](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L136-L149)

## Overview
Serializes PostgreSQL's JSON data into binary format for efficient network transmission using PostgreSQL's binary protocol.

## Definition

```c
Datum
json_send(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function converts PostgreSQL's internal JSON representation into a binary format suitable for transmission over PostgreSQL's binary protocol. This function is part of PostgreSQL's binary input/output infrastructure and is used when clients request data in binary format rather than text format. The binary protocol is more efficient for network transmission as it avoids the overhead of text parsing and formatting.

The function uses PostgreSQL's standard binary serialization functions to create a properly formatted binary representation. Since JSON is internally stored as text, the function essentially wraps the text data in PostgreSQL's binary format with appropriate length prefixes and metadata.

## Parameters / Member Variables
- : PostgreSQL function call context containing:
  - Argument 0: Text datum () containing the JSON data to be serialized

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts text argument with potential detoasting
  - : Buffer structure for building binary output
  - : Initializes binary output buffer for type serialization
  - : Sends text data to binary buffer with length information
  - : Macro to get pointer to variable-length data
  - : Macro to get size of variable-length data excluding header
  - : Finalizes binary buffer and returns bytea result
  - : Returns binary data as bytea to PostgreSQL
- Called from (representative examples):
  - PostgreSQL binary protocol handler when sending JSON data to clients
  - Binary data export functions
  - Replication and backup systems using binary format

## Notes and Other Information
- Part of PostgreSQL's binary protocol support for efficient client-server communication
- The binary format includes length information and is platform-independent
- More efficient than text format for network transmission and storage
- Handles both toasted and untoasted JSON values automatically
- The resulting binary data can be reconstructed using the corresponding  function
- Used primarily when clients connect using PostgreSQL's binary protocol mode

## Simplified Source

```c
Datum
json_send(PG_FUNCTION_ARGS)
{
    text *json_text = PG_GETARG_TEXT_PP(0);
    StringInfoData buf;

    // Initialize binary output buffer
    pq_begintypsend(&buf);

    // Send text data to buffer with length information
    pq_sendtext(&buf, VARDATA_ANY(json_text), VARSIZE_ANY_EXHDR(json_text));

    // Return finalized binary data
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}
```