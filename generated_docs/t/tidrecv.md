# tidrecv

## Location
src/backend/utils/adt/tid.c: 139 - 159

## Overview
The `tidrecv` function converts PostgreSQL's TID (tuple identifier) data type from external binary format to internal ItemPointer representation, used in binary protocol communication.

## Definition
```c
Datum tidrecv(PG_FUNCTION_ARGS)
```

## Detailed Description
The `tidrecv` function is responsible for deserializing TID values from PostgreSQL's binary wire protocol format. It reads binary data from a StringInfo buffer and reconstructs an ItemPointer structure. This function is used when TID values are transmitted in binary format over the network or stored in binary form, complementing the text-based `tidin` function. The function reads the block number and offset number sequentially from the binary stream and creates a new ItemPointer structure.

## Parameters / Member Variables
- Input parameter accessed via `PG_GETARG_POINTER(0)`: StringInfo buffer containing binary TID data
- Internal variables:
  - `buf`: StringInfo pointer to the binary data buffer
  - `result`: Newly allocated ItemPointer to hold the deserialized TID
  - `blockNumber`: Block number read from binary stream
  - `offsetNumber`: Offset number read from binary stream

## Dependencies
- Functions called/Symbols referenced:
  - `pq_getmsgint`: PostgreSQL function to read integer from binary message buffer
  - `palloc`: PostgreSQL memory allocation function
  - `ItemPointerSet`: Sets block and offset in ItemPointer structure
  - `PG_RETURN_ITEMPOINTER`: PostgreSQL macro to return ItemPointer datum
- Called from (representative examples):
  - PostgreSQL binary protocol handling when receiving TID parameters
  - Binary data deserialization in prepared statements with TID parameters
  - Internal communication between PostgreSQL processes using binary format

## Notes and Other Information
- This function assumes the binary data format matches the expected layout for TID values
- The function reads block number and offset number using their native sizes via `sizeof()` operators
- Memory for the result ItemPointer is allocated using `palloc` for proper PostgreSQL memory management
- Part of PostgreSQL's type system binary I/O infrastructure alongside `tidsend`
- No explicit validation is performed on the received values, assuming the binary format is trusted