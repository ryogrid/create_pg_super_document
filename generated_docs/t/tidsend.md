# tidsend

## Location
src/backend/utils/adt/tid.c: 160 - 175

## Overview
The `tidsend` function converts PostgreSQL's TID (tuple identifier) data type from internal ItemPointer representation to external binary format for network transmission.

## Definition
```c
Datum tidsend(PG_FUNCTION_ARGS)
```

## Detailed Description
The `tidsend` function serializes TID values into PostgreSQL's binary wire protocol format. It takes an ItemPointer structure and converts it into a binary representation suitable for network transmission or binary storage. This function is the counterpart to `tidrecv` and is used when TID values need to be sent in binary format over the network or stored in binary form. The function serializes the block number as a 32-bit integer and the offset number as a 16-bit integer into a binary buffer.

## Parameters / Member Variables
- Input parameter accessed via `PG_GETARG_ITEMPOINTER(0)`: ItemPointer structure to serialize to binary format
- Internal variables:
  - `itemPtr`: Pointer to the input ItemPointer structure
  - `buf`: StringInfoData buffer to hold the serialized binary data

## Dependencies
- Functions called/Symbols referenced:
  - [pq_begintypsend](../p/pq_begintypsend.md): Initializes binary output buffer for type serialization
  - [pq_sendint32](../p/pq_sendint32.md): Sends a 32-bit integer to the binary buffer
  - [pq_sendint16](../p/pq_sendint16.md): Sends a 16-bit integer to the binary buffer
  - [ItemPointerGetBlockNumberNoCheck](../I/ItemPointerGetBlockNumberNoCheck.md): Extracts block number from ItemPointer without validation
  - [ItemPointerGetOffsetNumberNoCheck](../I/ItemPointerGetOffsetNumberNoCheck.md): Extracts offset number from ItemPointer without validation
  - [pq_endtypsend](../p/pq_endtypsend.md): Finalizes binary output buffer and returns bytea
  - `PG_RETURN_BYTEA_P`: PostgreSQL macro to return bytea datum
- Called from (representative examples):
  - PostgreSQL binary protocol handling when sending TID values to clients
  - Binary data serialization in prepared statements with TID results
  - Internal communication between PostgreSQL processes using binary format

## Notes and Other Information
- The function uses fixed-size integer serialization: 32-bit for block number, 16-bit for offset number
- Uses "NoCheck" variants of ItemPointer accessor functions, assuming the input ItemPointer is valid
- The resulting binary format is platform-independent (network byte order)
- Part of PostgreSQL's type system binary I/O infrastructure alongside `tidrecv`
- The binary format produced by this function can be deserialized by the corresponding `tidrecv` function
- Memory management for the output buffer is handled automatically by the pq_*typsend functions