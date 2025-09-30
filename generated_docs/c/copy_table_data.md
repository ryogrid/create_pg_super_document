# copy_table_data

## Location
[src/backend/commands/cluster.c:814-1060](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/cluster.c#L814-L1060)

## Overview
Performs the physical copying of table data from an old heap to a new heap, handling tuple visibility, freezing, and transaction management during table reorganization operations.

## Definition

```c
struct VacuumCutoffs cutoffs;
```
## Detailed Description
The `copy_table_data` function is responsible for the actual data transfer during table clustering and rewriting operations. It handles complex aspects of PostgreSQL's MVCC system including:

1. **Data Transfer**: Copies all visible tuples from the old table to the new table
2. **Ordering**: Uses either index scan (for clustering) or sequential scan with optional sorting
3. **Transaction Management**: Computes appropriate freeze cutoff points for transaction IDs
4. **TOAST Handling**: Manages TOAST table relationships and decides between content vs. link swapping
5. **Statistics**: Updates table statistics (relpages, reltuples) in pg_class catalog

The function is access method (AM) agnostic, delegating the actual copying to AM-specific functions while handling the generic coordination tasks.

## Parameters / Member Variables
- `OIDNewHeap`: OID of the destination table to copy data into
- `OIDOldHeap`: OID of the source table to copy data from  
- `OIDOldIndex`: OID of index to use for ordering (InvalidOid for physical order)
- `verbose`: Boolean flag controlling logging verbosity
- `pSwapToastByContent`: Output parameter indicating whether TOAST swap should be by content
- `pFreezeXid`: Output parameter receiving the transaction ID used as freeze cutoff
- `pCutoffMulti`: Output parameter receiving the MultiXactId used as cutoff

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)/table_close: Opens and closes table relations
  - [index_open](../i/index_open.md)/index_close: Opens and closes index relations  
  - [LockRelationOid](../L/LockRelationOid.md): Locks TOAST table to prevent autovacuum interference
  - [vacuum_get_cutoffs](../v/vacuum_get_cutoffs.md): Computes freeze and MultiXact cutoff values
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md): Compares transaction IDs for cutoff calculations
  - [plan_cluster_use_sort](../p/plan_cluster_use_sort.md): Determines whether to use sort vs index scan
  - [table_relation_copy_for_cluster](../t/table_relation_copy_for_cluster.md): AM-specific data copying function
  - SearchSysCacheCopy1: Retrieves catalog information
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md): Updates system catalog entries
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md): Makes catalog changes visible
- Called from (representative examples):
  - [rebuild_relation](../r/rebuild_relation.md): Main table rebuilding function
  - RelToCluster: Cluster processing workflow

## Notes and Other Information
- Handles both clustered (index-ordered) and non-clustered (physical order) copying
- Manages TOAST table locking to prevent autovacuum race conditions
- Computes aggressive freeze cutoffs since the entire table is being rewritten
- Uses planner cost estimates to choose between index scan and sort methods
- Updates table statistics in pg_class to reflect the new table's characteristics  
- Preserves TOAST value OIDs when doing content-based TOAST swapping
- Provides detailed logging of the operation's progress and results
- Critical for maintaining data consistency during table reorganization operations

## Simplified Source

```c
static void
copy_table_data(Oid OIDNewHeap, Oid OIDOldHeap, Oid OIDOldIndex, bool verbose,
                bool *pSwapToastByContent, TransactionId *pFreezeXid,
                MultiXactId *pCutoffMulti)
{
    Relation NewHeap, OldHeap, OldIndex;
    Relation relRelation;
    HeapTuple reltup;
    Form_pg_class relform;
    struct VacuumCutoffs cutoffs;
    VacuumParams params;
    bool use_sort;
    double num_tuples = 0, tups_vacuumed = 0, tups_recently_dead = 0;
    BlockNumber num_pages;
    int elevel = verbose ? INFO : DEBUG2;

    // Open relations with exclusive locks
    NewHeap = table_open(OIDNewHeap, AccessExclusiveLock);
    OldHeap = table_open(OIDOldHeap, AccessExclusiveLock);
    if (OidIsValid(OIDOldIndex))
        OldIndex = index_open(OIDOldIndex, AccessExclusiveLock);
    else
        OldIndex = NULL;

    // Lock TOAST table to prevent autovacuum interference
    if (OldHeap->rd_rel->reltoastrelid)
        LockRelationOid(OldHeap->rd_rel->reltoastrelid, AccessExclusiveLock);

    // Determine TOAST swap strategy
    if (OldHeap->rd_rel->reltoastrelid && NewHeap->rd_rel->reltoastrelid) {
        *pSwapToastByContent = true;
        // Set new heap to use old toast table OID for content swap
        NewHeap->rd_toastoid = OldHeap->rd_rel->reltoastrelid;
    } else {
        *pSwapToastByContent = false;
    }

    // Compute freeze cutoffs for aggressive cleanup
    memset(&params, 0, sizeof(VacuumParams));
    vacuum_get_cutoffs(OldHeap, &params, &cutoffs);

    // Ensure freeze XID doesn't go backwards
    TransactionId relfrozenxid = OldHeap->rd_rel->relfrozenxid;
    if (TransactionIdIsValid(relfrozenxid) &&
        TransactionIdPrecedes(cutoffs.FreezeLimit, relfrozenxid))
        cutoffs.FreezeLimit = relfrozenxid;

    // Ensure MultiXact cutoff doesn't go backwards
    MultiXactId relminmxid = OldHeap->rd_rel->relminmxid;
    if (MultiXactIdIsValid(relminmxid) &&
        MultiXactIdPrecedes(cutoffs.MultiXactCutoff, relminmxid))
        cutoffs.MultiXactCutoff = relminmxid;

    // Choose scan method: index scan vs sequential scan with sort
    if (OldIndex != NULL && OldIndex->rd_rel->relam == BTREE_AM_OID)
        use_sort = plan_cluster_use_sort(OIDOldHeap, OIDOldIndex);
    else
        use_sort = false;

    // Log the chosen strategy
    char *nspname = get_namespace_name(RelationGetNamespace(OldHeap));
    if (OldIndex != NULL && !use_sort)
        ereport(elevel, (errmsg("clustering \"%s.%s\" using index scan on \"%s\"",
                               nspname, RelationGetRelationName(OldHeap),
                               RelationGetRelationName(OldIndex))));
    else if (use_sort)
        ereport(elevel, (errmsg("clustering \"%s.%s\" using sequential scan and sort",
                               nspname, RelationGetRelationName(OldHeap))));
    else
        ereport(elevel, (errmsg("vacuuming \"%s.%s\"",
                               nspname, RelationGetRelationName(OldHeap))));

    // Delegate actual copying to access method specific function
    table_relation_copy_for_cluster(OldHeap, NewHeap, OldIndex, use_sort,
                                   cutoffs.OldestXmin, &cutoffs.FreezeLimit,
                                   &cutoffs.MultiXactCutoff,
                                   &num_tuples, &tups_vacuumed,
                                   &tups_recently_dead);

    // Return freeze cutoffs to caller
    *pFreezeXid = cutoffs.FreezeLimit;
    *pCutoffMulti = cutoffs.MultiXactCutoff;

    // Reset TOAST OID
    NewHeap->rd_toastoid = InvalidOid;

    // Update pg_class statistics
    num_pages = RelationGetNumberOfBlocks(NewHeap);

    relRelation = table_open(RelationRelationId, RowExclusiveLock);
    reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(OIDNewHeap));
    if (!HeapTupleIsValid(reltup))
        elog(ERROR, "cache lookup failed for relation %u", OIDNewHeap);

    relform = (Form_pg_class) GETSTRUCT(reltup);
    relform->relpages = num_pages;
    relform->reltuples = num_tuples;

    // Update catalog (except for pg_class itself)
    if (OIDOldHeap != RelationRelationId)
        CatalogTupleUpdate(relRelation, &reltup->t_self, reltup);
    else
        CacheInvalidateRelcacheByTuple(reltup);

    // Cleanup and close relations
    heap_freetuple(reltup);
    table_close(relRelation, RowExclusiveLock);

    if (OldIndex != NULL)
        index_close(OldIndex, NoLock);
    table_close(OldHeap, NoLock);
    table_close(NewHeap, NoLock);

    // Log operation results
    ereport(elevel,
            (errmsg("\"%s.%s\": found %.0f removable, %.0f nonremovable row versions in %u pages",
                   nspname, RelationGetRelationName(OldHeap),
                   tups_vacuumed, num_tuples,
                   RelationGetNumberOfBlocks(OldHeap))));

    CommandCounterIncrement();
}
```