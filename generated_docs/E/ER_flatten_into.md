# ER_flatten_into

## Location
src/backend/utils/adt/expandedrecord.c: 764 - 823

## Overview
ER_flatten_into serializes an expanded record into a flattened composite datum format at a specified memory location.

## Definition
static void ER_flatten_into(ExpandedObjectHeader *eohptr, void *result, Size allocated_size)

## Detailed Description
This function flattens an expanded record into a standard HeapTuple format that can be stored or transmitted. It provides two optimization paths:

1. **Fast path**: If the record has a valid cached flattened representation without external references, it performs a simple memcpy operation and updates the datum header fields.

2. **Full reconstruction**: When no cached representation is available, it constructs the flattened tuple from scratch using the expanded records dvalues and dnulls arrays.

The function ensures proper initialization of all header fields including datum length, type information, and tuple metadata. It also guarantees that pad space is zero-filled for consistent binary representation.

## Parameters / Member Variables
- `eohptr`: Pointer to the ExpandedObjectHeader containing the expanded record to flatten
- `result`: Destination buffer where the flattened tuple will be written
- `allocated_size`: Size of the destination buffer (must match the size calculated by ER_get_flat_size)

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderSetDatumLength
  - HeapTupleHeaderSetTypeId
  - HeapTupleHeaderSetTypMod
  - expanded_record_get_tupdesc
  - ItemPointerSetInvalid
  - HeapTupleHeaderSetNatts
  - heap_fill_tuple
- Called from (representative examples):
  - No direct references found (likely called via function pointer in ExpandedObjectMethods)

## Notes and Other Information
- This is a method implementation for the expanded object infrastructure
- Uses memset to ensure all padding bytes are zero-filled for consistent binary representation
- Sets t_ctid to invalid value since flattened composite datums do not have valid tuple identifiers
- The fast path optimization avoids reconstruction when a valid cached representation exists
- Works in conjunction with ER_get_flat_size to ensure proper memory allocation
- Part of PostgreSQL's expanded object system for efficient serialization of complex data types