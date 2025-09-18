# RecordCompareData

## Location
src/backend/utils/adt/rowtypes.c: 59 - 67

## Overview
A structure that caches metadata needed for comparing entire records, supporting comparisons between records of potentially different types by maintaining type information for both operands.

## Definition


## Detailed Description
RecordCompareData is a comprehensive structure designed to optimize record comparison operations in PostgreSQL. Unlike simpler comparison scenarios, this structure is specifically designed to handle comparisons between records that may have different types, requiring separate type metadata for each operand (record1_type/record2_type).

The structure uses a flexible array member to store ColumnCompareData entries for each column, allowing efficient column-by-column comparison operations. This design is essential for PostgreSQL's record comparison functions, equality checks, hashing operations, and image-based comparisons, where the system needs to repeatedly compare records with cached type information rather than performing expensive type lookups for each comparison.

The dual-type design (record1_type and record2_type) enables PostgreSQL to handle heterogeneous record comparisons, such as when comparing records from different tables or composite types that may be structurally compatible but have different type OIDs.

## Parameters / Member Variables
- : The number of columns allocated in the columns array
- : The OID of the first record's type
- : Type modifier for the first record type
- : The OID of the second record's type  
- : Type modifier for the second record type
- : Flexible array of ColumnCompareData structures, one for each column to be compared

## Dependencies
- Functions called/Symbols referenced:
  - ColumnCompareData
  - FLEXIBLE_ARRAY_MEMBER

- Called from (representative examples):
  - record_cmp (src/backend/utils/adt/rowtypes.c:838)
  - record_eq (src/backend/utils/adt/rowtypes.c:1082)
  - record_image_cmp (src/backend/utils/adt/rowtypes.c:1346)
  - record_image_eq (src/backend/utils/adt/rowtypes.c:1592)
  - hash_record (src/backend/utils/adt/rowtypes.c:1803)
  - hash_record_extended (src/backend/utils/adt/rowtypes.c:1924)

## Notes and Other Information
- Supports heterogeneous record comparisons with separate type information for each operand
- Uses flexible array member pattern for efficient memory utilization
- Central to PostgreSQL's record comparison optimization strategy
- Supports various comparison operations: equality, ordering, hashing, and image-based comparisons
- Memory layout optimized for cache-efficient column-by-column comparison operations
- Located in src/backend/utils/adt/rowtypes.c at lines 59-67
- Essential for performance in scenarios involving frequent record comparisons, such as sorting and grouping operations