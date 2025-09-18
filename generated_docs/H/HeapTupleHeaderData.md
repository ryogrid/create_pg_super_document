# HeapTupleHeaderData

## Location
src/include/access/htup_details.h: 153 - 165

## Overview
HeapTupleHeaderData is the fundamental header structure for heap tuples in PostgreSQL, containing all metadata necessary for transaction visibility, tuple identification, and data layout information.

## Definition


## Detailed Description
HeapTupleHeaderData serves as the complete header structure for tuples stored in PostgreSQL heap files. It contains all the metadata required for MVCC (Multi-Version Concurrency Control), transaction visibility determination, tuple versioning, and data layout management.

The structure uses a union to support two different operational modes: heap tuples (normal table storage) and datum tuples (composite type values). The header includes transaction identifiers for MVCC, a tuple identifier for version chaining, various flag bits for tuple properties, and a flexible bitmap for tracking NULL values.

Key design considerations include support for speculative insertion tokens, tuple version chaining through t_ctid links, and compatibility with MinimalTupleData for certain operations.

## Parameters / Member Variables
- : Union containing either HeapTupleFields (for heap tuples) or DatumTupleFields (for datum tuples)
  - : Contains transaction IDs (t_xmin, t_xmax) and command ID information for heap tuples
  - : Contains type information for composite datum values
- : ItemPointerData pointing to current TID of this or newer tuple version, or speculative insertion token
- : 16-bit field containing number of attributes plus various flags
- : 16-bit field with various flag bits indicating tuple properties (null values, variable width attributes, external storage, locking information, etc.)
- : 8-bit field indicating size of header including bitmap and padding
- : Flexible array member containing bitmap of NULL values (only present when tuple has NULLs)

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleFields
  - DatumTupleFields
  - ItemPointerData
- Called from (representative examples):
  - expand_tuple
  - heap_form_tuple
  - heap_tuple_from_minimal_tuple
  - heap_xlog_insert
  - heap_xlog_multi_insert
  - heap_xlog_update

## Notes and Other Information
- The structure supports PostgreSQL's MVCC system by storing virtual fields Xmin, Cmin, Xmax, Cmax, and Xvac in optimized physical storage
- Transaction visibility is determined through the transaction ID fields in the HeapTupleFields union member
- The t_ctid field enables tuple version chaining - following the chain leads to the newest version of a row
- Speculative insertion tokens can be stored in t_ctid during uncertain insertions
- Fields from t_infomask2 onward must match MinimalTupleData structure for compatibility
- The structure size is 23 bytes plus the variable-length NULL bitmap
- Located in src/include/access/htup_details.h:153-181 with typedef in src/include/access/htup.h:21