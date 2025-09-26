# vacuum

## Location
src/backend/commands/vacuum.c: 479 - 716

## Overview
Internal entry point for autovacuum and the VACUUM/ANALYZE commands that orchestrates the processing of specified relations or all relevant tables in the database.

## Definition

```c
void
vacuum(List *relations, VacuumParams *params, BufferAccessStrategy bstrategy,
	   MemoryContext vac_context, bool isTopLevel)
```
## Detailed Description
The vacuum function serves as the core orchestration layer for both user-initiated and automatic vacuum/analyze operations. It handles transaction management, relation list processing, and coordinates the execution of vacuum and analyze operations across multiple relations.

Key responsibilities include:
- Transaction boundary management (determining when to use separate transactions vs. the outer transaction)
- Preventing recursive vacuum calls that could occur through hostile index expressions
- Building and expanding the list of relations to process based on input parameters
- Managing vacuum cost accounting and failsafe mechanisms
- Coordinating vacuum_rel() and analyze_rel() calls for each target relation
- Updating database-wide statistics (pg_database.datfrozenxid) after vacuum operations

The function implements sophisticated transaction handling logic: VACUUM operations always use separate transactions to release locks quickly, while ANALYZE operations may reuse the outer transaction depending on context. For autovacuum workers and multi-relation operations, separate transactions are preferred for better concurrency.

## Parameters / Member Variables
- : List of VacuumRelation structures to process, or NIL to process all relevant tables in the database
- : VacuumParams structure containing options and configuration parameters for the operation
- : BufferAccessStrategy for controlling shared buffer usage, or NULL for unrestricted access
- : MemoryContext for allocating vacuum-related data that persists across transactions
- : Boolean indicating if this is a top-level command (affects transaction block validation)

## Dependencies
- Functions called/Symbols referenced:
  - vacuum_rel (per-relation vacuum processing)
  - analyze_rel (per-relation analysis processing)
  - expand_vacuum_rel (relation expansion utility)
  - get_all_vacuum_rels (database-wide relation discovery)
  - PreventInTransactionBlock (transaction validation)
  - VacuumUpdateCosts (cost accounting initialization)
  - vac_update_datfrozenxid (database statistics updates)
- Called from (representative examples):
  - ExecVacuum (user command entry point)
  - autovacuum_do_vac_analyze (autovacuum worker)

## Notes and Other Information
- Uses static variable in_vacuum to prevent recursive calls that could occur through index expressions
- Implements comprehensive transaction management: VACUUM always uses separate transactions, ANALYZE behavior depends on context
- Maintains vacuum cost accounting variables (VacuumPageHit, VacuumPageMiss, VacuumPageDirty) for throttling
- Supports database-only statistics mode (VACOPT_ONLY_DATABASE_STATS) that skips table processing
- Uses PG_TRY/PG_FINALLY blocks to ensure proper cleanup of vacuum state variables
- Handles both single-relation and multi-relation operations with appropriate transaction boundaries
- Updates system-wide frozen transaction ID tracking after vacuum operations complete