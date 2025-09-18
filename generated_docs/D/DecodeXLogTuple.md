# DecodeXLogTuple

## Location
[src/backend/replication/logical/decode.c:1266-1311](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/decode.c#L1266-L1311)

## Overview
DecodeXLogTuple reconstructs a HeapTuple from its WAL representation as logged by heap operations like insert, update, and delete during logical replication decoding.

## Definition
```c
static void DecodeXLogTuple(char *data, Size len, HeapTuple tuple)
```

## Detailed Description
DecodeXLogTuple is a utility function that converts tuple data stored in write-ahead log records back into a complete HeapTuple structure. When PostgreSQL logs heap operations (INSERT, UPDATE, DELETE), it stores tuples in a compact format in the WAL. This function reconstructs the full tuple structure needed by logical replication.

The function handles the conversion from the WAL's xl_heap_header format to a proper HeapTuple with a HeapTupleHeader. It carefully manages memory alignment issues since WAL data may not be aligned, and sets up the tuple with appropriate metadata for logical replication processing.

Key operations:
1. Calculates the actual tuple data length (excluding WAL header)
2. Sets up the HeapTuple structure with proper length
3. Handles memory alignment by copying the xl_heap_header to aligned storage
4. Initializes the HeapTupleHeader with zeroed memory
5. Copies the tuple data after the header
6. Transfers infomask flags and header offset from the WAL representation

## Parameters / Member Variables
- `data`: Raw pointer to the tuple data as stored in the WAL record
- `len`: Total length of the data including the xl_heap_header
- `tuple`: Pre-allocated HeapTuple structure to populate with the reconstructed data

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md)
  - [xl_heap_header](../x/xl_heap_header.md) (struct)
  - HeapTupleHeader (type)
  - SizeOfHeapHeader
  - SizeofHeapTupleHeader
- Called from (representative examples):
  - [DecodeInsert](DecodeInsert.md)
  - [DecodeUpdate](DecodeUpdate.md)
  - [DecodeDelete](DecodeDelete.md)

## Notes and Other Information
- This function is specifically NOT used by heap_multi_insert operations, which have their own decoding logic
- The function handles unaligned WAL data by explicitly copying to aligned storage before processing
- Sets t_self to invalid since this is not a disk-based tuple during logical replication
- Sets t_tableOid to InvalidOid initially - the correct OID is determined later during transaction reassembly
- The function assumes the caller has pre-allocated the HeapTuple and its t_data buffer with sufficient space
- Critical for maintaining tuple structure integrity when converting from compact WAL format to full heap tuple representation
- Used across all major heap operation decoders, making it a central utility for logical replication tuple reconstruction