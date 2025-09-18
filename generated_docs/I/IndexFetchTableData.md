# IndexFetchTableData

## Location
src/include/access/relscan.h: 104 - 107

## Overview
A base class structure for fetching tuples from a table via an index, designed to be embedded in access method-specific structures for index-based table access operations.

## Definition
```c
typedef struct IndexFetchTableData
{
    Relation    rel;
} IndexFetchTableData;
```

## Detailed Description
IndexFetchTableData serves as a foundational structure for implementing index-based table tuple fetching across different access methods in PostgreSQL. This structure provides a common interface that individual access methods (AMs) can extend by embedding it within their own specialized structures. The design follows an object-oriented pattern where this base class contains the essential relation reference, while derived structures in specific access methods add their own state and functionality. This approach enables polymorphic behavior for index fetch operations while maintaining type safety and code reusability across different storage engines.

## Parameters / Member Variables
- `rel`: Relation - A reference to the table relation being accessed through the index, providing access to relation metadata, schema information, and storage details

## Dependencies
- Functions called/Symbols referenced:
  - [Relation](../R/Relation.md) (from src/include/utils/relcache.h)
- Called from (representative examples):
  - [heapam_index_fetch_reset](../h/heapam_index_fetch_reset.md)
  - [heapam_index_fetch_end](../h/heapam_index_fetch_end.md)  
  - [heapam_index_fetch_tuple](../h/heapam_index_fetch_tuple.md)
  - [table_index_fetch_tuple_check](../t/table_index_fetch_tuple_check.md)
  - [IndexFetchHeapData](IndexFetchHeapData.md) (as embedded member)
  - [IndexScanDescData](IndexScanDescData.md) (as embedded member)

## Notes and Other Information
- This is a base class intended for inheritance-style usage in C through structure embedding
- Individual access methods create their own structures that include this as the first member
- The structure enables polymorphic dispatch for index fetch operations across different table access methods
- Essential component of PostgreSQL's pluggable storage engine architecture
- Used in conjunction with index scan operations to efficiently retrieve table tuples based on index lookups
- The Relation member provides access to table metadata needed for tuple reconstruction and validation