# do_autovacuum

## Location
src/backend/postmaster/autovacuum.c: 1877 - 2587

## Overview
do_autovacuum is the main function that processes an entire database table-by-table, determining which tables need vacuum or analyze operations and performing autovacuum work items.

## Definition


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
  - StartTransactionCommand/CommitTransactionCommand (transaction management)
  - table_beginscan_catalog/table_endscan/heap_getnext (catalog scanning)
  - extract_autovac_opts (extract autovacuum options from relation)
  - relation_needs_vacanalyze (determine if table needs maintenance)
  - table_recheck_autovac (recheck table maintenance needs)
  - perform_work_item (process additional work items)
  - autovacuum_do_vac_analyze (perform actual vacuum/analyze)
  - AbortOutOfAnyTransaction (error recovery)
- Called from (representative examples):
  - AutoVacWorkerMain (main autovacuum worker entry point)

## Notes and Other Information
- Uses memory contexts (AutovacMemCxt, PortalContext) to manage memory allocation across transactions
- Implements careful locking protocols to avoid conflicts with concurrent autovacuum workers
- Handles configuration reloads dynamically during processing
- Supports both regular tables and TOAST tables with separate processing logic
- Includes comprehensive error handling with transaction abort and recovery
- Updates global vacuum cost parameters and worker balancing information
- Skips template and non-connectable databases by using zero freeze ages