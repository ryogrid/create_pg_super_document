# BrinValues

## Location
src/include/access/brin_tuple.h: 29 - 38

## Overview
BrinValues is a structure that represents the summary information for a single indexed column within a BRIN index tuple, storing accumulated values and metadata for efficient range-based indexing.

## Definition


## Detailed Description
BrinValues is a fundamental data structure in PostgreSQL's BRIN (Block Range Index) implementation. Each BRIN index tuple contains one BrinValues struct for each indexed column. This structure accumulates summary information about values within a page range, enabling efficient range-based queries. The structure supports various BRIN operator classes (minmax, inclusion, bloom) by storing opclass-specific summary data in the bv_values array. The size and interpretation of this array depends on the specific operator class being used.

## Parameters / Member Variables
- : The attribute number of the indexed column this BrinValues represents
- : Boolean flag indicating whether any NULL values exist in the page range
- : Boolean flag indicating whether all values in the page range are NULL
- : Pointer to an array of Datum values containing the accumulated summary data (size determined by opclass)
- : Expanded form of accumulated values, used for in-memory operations
- : Memory context used for managing the lifetime of associated data
- : Callback function pointer for serializing the values to disk format

## Dependencies
- Functions called/Symbols referenced:
  - AttrNumber (data type)
  - Datum (data type)
  - MemoryContext (data type)
  - brin_serialize_callback_type (function pointer type)
- Called from (representative examples):
  - brin_bloom_add_value
  - brin_inclusion_add_value
  - brin_minmax_add_value
  - brin_minmax_multi_add_value
  - union_tuples
  - add_values_to_range
  - brin_new_memtuple
  - brin_memtuple_initialize

## Notes and Other Information
- Each BRIN index tuple stores one BrinValues struct per indexed column
- The bv_values array size and content format is determined by the specific BRIN operator class
- Used extensively across different BRIN operator class implementations (minmax, inclusion, bloom, minmax_multi)
- Critical for BRIN's space-efficient summarization of large page ranges
- The structure supports both on-disk and in-memory representations through bv_values and bv_mem_value