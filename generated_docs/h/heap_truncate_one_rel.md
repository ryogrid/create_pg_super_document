# heap_truncate_one_rel

## Location
[src/backend/catalog/heap.c:3110-3153](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L3110-L3153)

## Overview
heap_truncate_one_rel deletes all data within a single specified relation, including its indexes and associated TOAST table if present.

## Definition
void heap_truncate_one_rel(Relation rel)

## Detailed Description
This function performs a complete truncation of a single relation by removing all data from the main table, its indexes, and any associated TOAST table. It operates in a non-transactional manner, meaning the truncation is immediate and cannot be rolled back. The function handles different relation types appropriately - partitioned tables are skipped since they have no storage, while regular tables undergo full truncation including their storage files.

The truncation process involves: (1) truncating the underlying relation storage using table_relation_nontransactional_truncate, (2) truncating all associated indexes via RelationTruncateIndexes, and (3) if present, truncating the TOAST table and its indexes. The caller must have already obtained AccessExclusiveLock on the relation and verified permissions.

## Parameters / Member Variables
- : The relation to be truncated (caller must hold AccessExclusiveLock)

## Dependencies
- Functions called/Symbols referenced:
  - [table_relation_nontransactional_truncate](../t/table_relation_nontransactional_truncate.md)
  - [RelationTruncateIndexes](../R/RelationTruncateIndexes.md)
  - OidIsValid
  - [table_open](../t/table_open.md)
  - AccessExclusiveLock
  - [table_close](../t/table_close.md)
  - NoLock
- Called from (representative examples):
  - [heap_truncate](heap_truncate.md)
  - [ExecuteTruncateGuts](../E/ExecuteTruncateGuts.md)

## Notes and Other Information
- This function is NOT transaction-safe and cannot be rolled back
- Caller must hold AccessExclusiveLock on the relation before calling
- Partitioned tables are handled as no-ops since they have no physical storage
- TOAST tables and their indexes are automatically truncated if they exist
- Used by both the non-transactional heap_truncate and the transactional ExecuteTruncateGuts
- The function maintains locks on TOAST tables until transaction end to prevent concurrent access

## Simplified Source

```c
void heap_truncate_one_rel(Relation rel) {
    // Skip partitioned tables - they have no storage
    if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
        return;

    // Truncate the main relation storage
    table_relation_nontransactional_truncate(rel);

    // Truncate all indexes on the relation
    RelationTruncateIndexes(rel);

    // Handle TOAST table if present
    Oid toastrelid = rel->rd_rel->reltoastrelid;
    if (OidIsValid(toastrelid)) {
        Relation toastrel = table_open(toastrelid, AccessExclusiveLock);

        // Truncate TOAST table and its indexes
        table_relation_nontransactional_truncate(toastrel);
        RelationTruncateIndexes(toastrel);

        table_close(toastrel, NoLock);  // Keep lock until transaction end
    }
}
```