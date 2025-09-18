# PartitionHasPendingDetach

## Location
[src/backend/catalog/pg_inherits.c:620-656](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_inherits.c#L620-L656)

## Overview
Checks whether a partition has a pending detach operation by examining the inhdetachpending flag in the pg_inherits catalog.

## Definition
```c
bool PartitionHasPendingDetach(Oid partoid)
```

## Detailed Description
This function determines if a partition has a pending detach operation by looking up its entry in the pg_inherits system catalog and checking the inhdetachpending flag. This is used in partition management to track concurrent detach operations and prevent conflicts.

The function performs these steps:
1. Opens the pg_inherits catalog with RowExclusiveLock
2. Scans for the partition's entry using inhrelid
3. Checks the inhdetachpending flag in the found tuple
4. Returns true if detach is pending, false otherwise
5. Raises an error if no inheritance entry is found (indicating it's not a partition)

The function assumes there should only be one pg_inherits entry for a partition (since partitions have exactly one parent), but doesn't explicitly verify this assumption.

## Parameters / Member Variables
- `partoid`: OID of the partition relation to check for pending detach status

## Dependencies
- Functions called/Symbols referenced:
  - [systable_beginscan](../s/systable_beginscan.md): Begins system table scan on pg_inherits
  - [systable_getnext](../s/systable_getnext.md): Gets next tuple from the system scan  
  - Form_pg_inherits: Accesses structured data from pg_inherits tuple
- Called from (representative examples):
  - [ATPrepCmd](../A/ATPrepCmd.md): During ALTER TABLE command preparation to check partition state

## Notes and Other Information
- The function comments note there's no good way to verify the relation is actually a partition before checking
- Assumes partitions have exactly one inheritance entry (which should be true for well-formed partitions)
- Uses RowExclusiveLock on pg_inherits, which may seem heavy for a read operation but ensures consistency
- Raises an ERROR if the relation is not found in pg_inherits, confirming it's not a partition
- Critical for managing concurrent partition detach operations in PostgreSQL's partitioning system
- The inhdetachpending flag is part of PostgreSQL's mechanism for safe concurrent partition management
- Location: src/backend/catalog/pg_inherits.c:620-656