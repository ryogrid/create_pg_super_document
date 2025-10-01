# swap_relation_files

## Location
[src/backend/commands/cluster.c:1061-1437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/cluster.c#L1061-L1437)

## Overview
Swaps the physical files of two relations while maintaining their logical identities, handling both regular and mapped relations along with associated TOAST tables and indexes.

## Definition

```c
enumber1,
				relfilenumber2;
```
## Detailed Description
The `swap_relation_files` function is a critical component of PostgreSQL's table reorganization operations that swaps the physical storage identities of two relations while preserving their logical identities. This allows the system to atomically replace an old table with a reorganized version.

Key operations performed:
1. **Physical Identity Swap**: Exchanges relfilenumber, reltablespace, relam, and relpersistence between relations
2. **Mapped Relations**: For system catalogs, updates the relation mapping instead of pg_class entries
3. **TOAST Handling**: Supports both content-based and link-based TOAST table swapping
4. **Statistics Transfer**: Exchanges table statistics (relpages, reltuples, relallvisible)
5. **Dependency Management**: Updates access method and TOAST table dependencies as needed
6. **Freeze Information**: Sets new freeze transaction ID and MultiXact cutoff values

The function handles the complexity of PostgreSQL's dual approach to relation storage (regular vs mapped relations) and ensures atomicity of the swap operation.

## Parameters / Member Variables
- `r1`: OID of the first relation to swap (typically the target relation)
- `r2`: OID of the second relation to swap (typically the temporary relation)
- `target_is_pg_class`: Boolean indicating if we're swapping pg_class itself (special case)
- `swap_toast_by_content`: Boolean controlling TOAST table swapping method (content vs links)
- `is_internal`: Boolean indicating if this is an internal operation (affects hooks)
- `frozenXid`: Transaction ID to set as the new freeze cutoff for r1
- `cutoffMulti`: MultiXact ID to set as the new cutoff for r1
- `mapped_tables`: Output array to collect OIDs of mapped tables involved

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheCopy1: Retrieves pg_class tuples for both relations
  - [RelationMapOidToFilenumber](../R/RelationMapOidToFilenumber.md)/RelationMapUpdateMap: Handles mapped relation file swapping
  - [relation_open](../r/relation_open.md)/relation_close: Opens relations to update subtransaction tracking
  - [RelationAssumeNewRelfilelocator](../R/RelationAssumeNewRelfilelocator.md): Marks relation as having new storage in subtransaction
  - [CatalogOpenIndexes](../C/CatalogOpenIndexes.md)/CatalogTupleUpdateWithInfo: Updates pg_class entries with index maintenance
  - [changeDependencyFor](../c/changeDependencyFor.md): Updates access method dependencies when they differ
  - [deleteDependencyRecordsFor](../d/deleteDependencyRecordsFor.md)/recordDependencyOn: Manages TOAST table dependencies
  - [toast_get_valid_index](../t/toast_get_valid_index.md): Retrieves TOAST table indexes for content swapping
  - InvokeObjectPostAlterHookArg: Fires post-alter hooks for both relations
- Called from (representative examples):
  - [swap_relation_files](swap_relation_files.md): Recursive calls for TOAST tables and indexes
  - [finish_heap_swap](../f/finish_heap_swap.md): Main heap swapping coordination

## Notes and Other Information
- Handles both regular relations (via pg_class updates) and mapped relations (via relation mapper)
- For mapped relations, enforces restrictions on tablespace, persistence, and access method changes
- Supports recursive swapping of TOAST tables and their indexes when using content-based swapping
- Updates subtransaction tracking to ensure proper cleanup on rollback
- Special handling for pg_class swaps to avoid updating data that will be discarded
- Maintains dependency information for TOAST tables when using link-based swapping
- Critical for the atomicity of table reorganization operations like CLUSTER and ALTER TABLE
- The function is recursive - it calls itself to handle TOAST table and index swapping

## Simplified Source

```c
static void
swap_relation_files(Oid r1, Oid r2, bool target_is_pg_class,
                    bool swap_toast_by_content, bool is_internal,
                    TransactionId frozenXid, MultiXactId cutoffMulti,
                    Oid *mapped_tables)
{
    Relation relRelation;
    HeapTuple reltup1, reltup2;
    Form_pg_class relform1, relform2;
    RelFileNumber relfilenumber1, relfilenumber2;

    // Open pg_class and get tuples for both relations
    relRelation = table_open(RelationRelationId, RowExclusiveLock);

    reltup1 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(r1));
    if (!HeapTupleIsValid(reltup1))
        elog(ERROR, "cache lookup failed for relation %u", r1);
    relform1 = (Form_pg_class) GETSTRUCT(reltup1);

    reltup2 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(r2));
    if (!HeapTupleIsValid(reltup2))
        elog(ERROR, "cache lookup failed for relation %u", r2);
    relform2 = (Form_pg_class) GETSTRUCT(reltup2);

    relfilenumber1 = relform1->relfilenode;
    relfilenumber2 = relform2->relfilenode;

    // Handle file number swapping differently for mapped vs regular relations
    if (RelFileNumberIsValid(relfilenumber1) && RelFileNumberIsValid(relfilenumber2)) {
        // Normal relations: swap physical attributes in pg_class

        // Swap file numbers, tablespaces, access methods, persistence
        RelFileNumber temp = relform1->relfilenode;
        relform1->relfilenode = relform2->relfilenode;
        relform2->relfilenode = temp;

        temp = relform1->reltablespace;
        relform1->reltablespace = relform2->reltablespace;
        relform2->reltablespace = temp;

        temp = relform1->relam;
        relform1->relam = relform2->relam;
        relform2->relam = temp;

        char tmpchr = relform1->relpersistence;
        relform1->relpersistence = relform2->relpersistence;
        relform2->relpersistence = tmpchr;

        // Swap TOAST links if using link-based swapping
        if (!swap_toast_by_content) {
            temp = relform1->reltoastrelid;
            relform1->reltoastrelid = relform2->reltoastrelid;
            relform2->reltoastrelid = temp;
        }
    } else {
        // Mapped relations: update relation mappings instead

        // Validate that both relations are mapped and compatible
        if (RelFileNumberIsValid(relfilenumber1) || RelFileNumberIsValid(relfilenumber2))
            elog(ERROR, "cannot swap mapped relation with non-mapped relation");

        // Get current mappings and update them with swapped values
        relfilenumber1 = RelationMapOidToFilenumber(r1, relform1->relisshared);
        relfilenumber2 = RelationMapOidToFilenumber(r2, relform2->relisshared);

        RelationMapUpdateMap(r1, relfilenumber2, relform1->relisshared, false);
        RelationMapUpdateMap(r2, relfilenumber1, relform2->relisshared, false);

        *mapped_tables++ = r2;  // Track mapped tables for caller
    }

    // Update subtransaction tracking for proper cleanup
    {
        Relation rel1 = relation_open(r1, NoLock);
        Relation rel2 = relation_open(r2, NoLock);
        rel2->rd_createSubid = rel1->rd_createSubid;
        rel2->rd_newRelfilelocatorSubid = rel1->rd_newRelfilelocatorSubid;
        rel2->rd_firstRelfilelocatorSubid = rel1->rd_firstRelfilelocatorSubid;
        RelationAssumeNewRelfilelocator(rel1);
        relation_close(rel1, NoLock);
        relation_close(rel2, NoLock);
    }

    // Set freeze information for non-index relations
    if (relform1->relkind != RELKIND_INDEX) {
        relform1->relfrozenxid = frozenXid;
        relform1->relminmxid = cutoffMulti;
    }

    // Swap table statistics (pages, tuples, all-visible pages)
    {
        int32 temp_pages = relform1->relpages;
        relform1->relpages = relform2->relpages;
        relform2->relpages = temp_pages;

        float4 temp_tuples = relform1->reltuples;
        relform1->reltuples = relform2->reltuples;
        relform2->reltuples = temp_tuples;

        int32 temp_allvisible = relform1->relallvisible;
        relform1->relallvisible = relform2->relallvisible;
        relform2->relallvisible = temp_allvisible;
    }

    // Update pg_class entries (unless we're swapping pg_class itself)
    if (!target_is_pg_class) {
        CatalogIndexState indstate = CatalogOpenIndexes(relRelation);
        CatalogTupleUpdateWithInfo(relRelation, &reltup1->t_self, reltup1, indstate);
        CatalogTupleUpdateWithInfo(relRelation, &reltup2->t_self, reltup2, indstate);
        CatalogCloseIndexes(indstate);
    } else {
        // Invalidate cache for pg_class swaps
        CacheInvalidateRelcacheByTuple(reltup1);
        CacheInvalidateRelcacheByTuple(reltup2);
    }

    // Update access method dependencies if they changed
    if (relform1->relam != relform2->relam) {
        changeDependencyFor(RelationRelationId, r1, AccessMethodRelationId,
                           relform1->relam, relform2->relam);
        changeDependencyFor(RelationRelationId, r2, AccessMethodRelationId,
                           relform2->relam, relform1->relam);
    }

    // Handle TOAST tables recursively
    if (relform1->reltoastrelid || relform2->reltoastrelid) {
        if (swap_toast_by_content) {
            // Recursively swap TOAST table contents
            if (relform1->reltoastrelid && relform2->reltoastrelid) {
                swap_relation_files(relform1->reltoastrelid, relform2->reltoastrelid,
                                   target_is_pg_class, swap_toast_by_content, is_internal,
                                   frozenXid, cutoffMulti, mapped_tables);
            }
        } else {
            // Update TOAST table dependencies for link-based swapping
            // (Simplified - update dependency records)
            if (relform1->reltoastrelid) {
                deleteDependencyRecordsFor(RelationRelationId, relform1->reltoastrelid, false);
                recordDependencyOn(/* new TOAST->base dependency */);
            }
            if (relform2->reltoastrelid) {
                deleteDependencyRecordsFor(RelationRelationId, relform2->reltoastrelid, false);
                recordDependencyOn(/* new TOAST->base dependency */);
            }
        }
    }

    // For TOAST tables, also swap their indexes
    if (swap_toast_by_content &&
        relform1->relkind == RELKIND_TOASTVALUE &&
        relform2->relkind == RELKIND_TOASTVALUE) {
        Oid toastIndex1 = toast_get_valid_index(r1, AccessExclusiveLock);
        Oid toastIndex2 = toast_get_valid_index(r2, AccessExclusiveLock);

        swap_relation_files(toastIndex1, toastIndex2, target_is_pg_class,
                           swap_toast_by_content, is_internal,
                           InvalidTransactionId, InvalidMultiXactId, mapped_tables);
    }

    // Clean up
    heap_freetuple(reltup1);
    heap_freetuple(reltup2);
    table_close(relRelation, RowExclusiveLock);
}
```