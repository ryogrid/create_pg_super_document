# DimensionInfo

## Location
[src/include/statistics/extended_stats_internal.h:34-41](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/statistics/extended_stats_internal.h#L34-L41)

## Overview
DimensionInfo is a structure that stores metadata for serialization and deserialization of dimension data in PostgreSQL's extended statistics framework, particularly for most common values (MCV) statistics.

## Definition


## Detailed Description
DimensionInfo serves as a metadata container that tracks important information needed for proper serialization and deserialization of dimension data in extended statistics. This structure is primarily used in the MCV (Most Common Values) statistics implementation to maintain type information and size calculations for efficient storage and retrieval of statistical data. The structure ensures that data can be properly reconstructed with correct alignment and type handling during deserialization operations.

## Parameters / Member Variables
- : The number of deduplicated values in this dimension
- : The total number of bytes required for the serialized representation
- : The size of the deserialized data including proper memory alignment
- : The type length from pg_type.typlen, indicating the storage size of the data type
- : Boolean flag from pg_type.typbyval, indicating whether the type is passed by value or reference

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references from this struct)
- Called from (representative examples):
  - SizeOfMCVList (src/backend/statistics/mcv.c:70)
  - [statext_mcv_serialize](../s/statext_mcv_serialize.md) (src/backend/statistics/mcv.c:628, 649, 820, 859, 860)
  - [statext_mcv_deserialize](../s/statext_mcv_deserialize.md) (src/backend/statistics/mcv.c:1008, 1100, 1102, 1103)

## Notes and Other Information
- This structure is part of PostgreSQL's extended statistics internal implementation
- Located in src/include/statistics/extended_stats_internal.h
- Primarily used for MCV list serialization/deserialization operations
- The alignment information (nbytes_aligned) is crucial for proper memory layout on different architectures
- Type information (typlen, typbyval) ensures correct handling of different PostgreSQL data types during statistics operations