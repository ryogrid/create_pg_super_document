# do_autovacuum

## Location
[src/backend/postmaster/autovacuum.c:1877-2587](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/autovacuum.c#L1877-L2587)

## Overview
do_autovacuum is the main function that processes an entire database table-by-table, determining which tables need vacuum or analyze operations and performing autovacuum work items.

## Definition

```c
static void
do_autovacuum(void)
```
## Detailed Description
This function serves as the core engine of the PostgreSQL autovacuum worker process. It scans the pg_class system catalog to identify tables that require maintenance operations (vacuum or analyze) based on their statistics and configuration parameters. The function operates in two main phases:

1. **Table Discovery Phase**: Scans pg_class in two passes - first for regular tables and materialized views, then for TOAST tables. It builds a list of table OIDs that need maintenance and creates a mapping between main tables and their associated TOAST tables.

2. **Processing Phase**: Iterates through the collected tables, performs necessary locking checks to avoid conflicts with concurrent workers, and executes vacuum/analyze operations through autovacuum_do_vac_analyze().

The function also handles orphaned temporary tables by detecting and dropping them, processes additional work items requested by backends, and updates the database's frozen XID information.

## Parameters / Member Variables
This function takes no parameters but operates on several important local variables:
- : List of table OIDs that need vacuum or analyze
- : List of orphaned temporary table OIDs to be dropped
- : Hash table mapping TOAST table OIDs to their main table information
- : Computed freeze age threshold for multixacts
- : Buffer access strategy for vacuum operations
- : Flag indicating whether any vacuum work was performed

## Dependencies
- Functions called/Symbols referenced:
  - [StartTransactionCommand](../S/StartTransactionCommand.md)/CommitTransactionCommand (transaction management)
  - [table_beginscan_catalog](../t/table_beginscan_catalog.md)/table_endscan/heap_getnext (catalog scanning)
  - [extract_autovac_opts](../e/extract_autovac_opts.md) (extract autovacuum options from relation)
  - [relation_needs_vacanalyze](../r/relation_needs_vacanalyze.md) (determine if table needs maintenance)
  - [table_recheck_autovac](../t/table_recheck_autovac.md) (recheck table maintenance needs)
  - [perform_work_item](../p/perform_work_item.md) (process additional work items)
  - [autovacuum_do_vac_analyze](../a/autovacuum_do_vac_analyze.md) (perform actual vacuum/analyze)
  - [AbortOutOfAnyTransaction](../A/AbortOutOfAnyTransaction.md) (error recovery)
- Called from (representative examples):
  - [AutoVacWorkerMain](../A/AutoVacWorkerMain.md) (main autovacuum worker entry point)

## Notes and Other Information
- Uses memory contexts (AutovacMemCxt, PortalContext) to manage memory allocation across transactions
- Implements careful locking protocols to avoid conflicts with concurrent autovacuum workers
- Handles configuration reloads dynamically during processing
- Supports both regular tables and TOAST tables with separate processing logic
- Includes comprehensive error handling with transaction abort and recovery
- Updates global vacuum cost parameters and worker balancing information
- Skips template and non-connectable databases by using zero freeze ages

## Simplified Source

```c
static void
do_autovacuum(void)
{
    List *table_oids = NIL;
    List *orphan_oids = NIL;
    HTAB *table_toast_map;
    BufferAccessStrategy bstrategy;
    bool did_vacuum = false;
    bool found_concurrent_worker = false;

    // Setup memory contexts and transaction
    AutovacMemCxt = AllocSetContextCreate(TopMemoryContext, "Autovacuum worker", ALLOCSET_DEFAULT_SIZES);
    StartTransactionCommand();

    // Get database settings and freeze age thresholds
    effective_multixact_freeze_max_age = MultiXactMemberFreezeThreshold();
    // Set freeze ages based on database template/connection status

    // Create TOAST-to-main table mapping
    table_toast_map = hash_create("TOAST to main relid map", 100, &ctl, HASH_ELEM | HASH_BLOBS);

    // Phase 1: Scan for regular tables and materialized views
    classRel = table_open(RelationRelationId, AccessShareLock);
    relScan = table_beginscan_catalog(classRel, 0, NULL);
    while ((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
    {
        // Skip non-target relation types, temp tables from other backends
        // Extract autovac options and check if vacuum/analyze needed
        // Add to table_oids list if maintenance required
        // Build TOAST mapping for second pass
    }
    table_endscan(relScan);

    // Phase 2: Scan TOAST tables specifically
    // Use similar logic but inherit options from main table if needed

    table_close(classRel, AccessShareLock);

    // Phase 3: Clean up orphaned temp tables
    foreach(cell, orphan_oids)
    {
        // Lock table, verify it's still orphaned, and drop if so
    }

    // Phase 4: Process collected tables
    bstrategy = GetAccessStrategyWithSize(BAS_VACUUM, VacuumBufferUsageLimit);
    foreach(cell, table_oids)
    {
        // Check for interrupts and config changes
        // Verify table still exists and get shared/local status
        // Check for concurrent workers and claim table
        // Recheck if table still needs maintenance
        // Set up cost balancing and perform vacuum/analyze
        // Handle errors and continue with next table
    }

    // Phase 5: Process additional work items
    for (i = 0; i < NUM_WORKITEMS; i++)
    {
        // Process pending work items for this database
        perform_work_item(workitem);
    }

    // Update database frozen XID if work was done
    if (did_vacuum || !found_concurrent_worker)
        vac_update_datfrozenxid();

    CommitTransactionCommand();
}
```