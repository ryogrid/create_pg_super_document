# RangeDelete

## Location
[src/backend/catalog/pg_range.c:113-138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_range.c#L113-L138)

## Overview
Removes the pg_range catalog entry for a specified range type when the range type is being dropped from the database.

## Definition

```c
void
RangeDelete(Oid rangeTypeOid)
```
## Detailed Description
RangeDelete is responsible for removing range type metadata from the pg_range system catalog when a range type is being deleted. The function performs a systematic scan of the pg_range table to locate entries matching the specified range type OID and deletes them from the catalog.

The function operates through the following steps:
1. Opens the pg_range catalog table with exclusive row lock
2. Initializes a scan key to search for entries with the specified range type OID
3. Begins a system catalog scan using the RangeTypidIndexId index for efficient lookup
4. Iterates through all matching tuples (should typically be just one)
5. Deletes each matching tuple using CatalogTupleDelete
6. Ends the scan and closes the catalog table

This function is typically called as part of the DROP TYPE command processing for range types.

## Parameters / Member Variables
- : The OID of the range type whose pg_range entry should be removed

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [systable_endscan](../s/systable_endscan.md)
  - table_close
- Called from (representative examples):
  - [RemoveTypeById](RemoveTypeById.md)

## Notes and Other Information
- The function uses the RangeTypidIndexId index to efficiently locate range entries by type OID
- The while loop structure allows for potential multiple matching entries, though typically there should be exactly one entry per range type
- The function acquires RowExclusiveLock on the pg_range table to ensure exclusive access during deletion
- This is part of the cleanup process when dropping range types and ensures catalog consistency
- The dependency system should ensure that dependent objects are handled appropriately before this function is called