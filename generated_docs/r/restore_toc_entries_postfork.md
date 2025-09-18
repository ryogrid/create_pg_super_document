# restore_toc_entries_postfork

## Location
src/bin/pg_dump/pg_backup_archiver.c: 4395 - 4428

## Overview
Final cleanup phase of parallel restore that processes any remaining TOC entries serially to handle cases where parallel processing couldn't complete due to circular dependencies or other issues.

## Definition


## Detailed Description
This function implements the third and final phase of PostgreSQL's three-phase parallel restore system. It serves as a safety net to handle TOC entries that couldn't be processed during the parallel phase, typically due to circular dependencies, deadlocks, or other pathological conditions that prevent normal dependency resolution. The function reconnects a single parent database connection and processes any remaining items serially, without concern for RestorePass ordering since the normal restore sequence has likely already been disrupted.

This phase normally should have no work to do if the parallel phase completed successfully, but it provides a mechanism to salvage the restoration process when parallel processing gets stuck or encounters unforeseen issues.

## Parameters / Member Variables
- `AH`: ArchiveHandle containing the restore context and database connection information
- `pending_list`: TocEntry list containing any items that remain unprocessed after the parallel phase

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_debug (logging function for debug messages)
  - ConnectDatabase (establishes database connection for the parent process)
  - _doSetFixedOutputState (re-establishes fixed database state settings)
  - pg_log_info (logging function for informational messages about missed items)
  - restore_toc_entry (processes individual TOC entry restoration)
  - RestoreOptions (structure containing restoration configuration parameters)
- Called from (representative examples):
  - RestoreArchive (main restore orchestration function)

## Notes and Other Information
- Third and final phase of the three-phase parallel restore system
- Acts as a fallback mechanism for items that couldn't be processed in parallel
- Reconnects the parent database connection that was closed after the prefork phase
- Re-establishes fixed database state (schema, user, tablespace settings) via _doSetFixedOutputState
- Processes items serially without regard to RestorePass ordering constraints
- Logs items as "missed items" to indicate they weren't processed during normal parallel execution
- Typically should have no work to do in successful restore operations
- Provides resilience against circular dependencies and other edge cases that could block parallel processing
- Uses the same restore_toc_entry function as other phases for consistency
- Iterates through pending_list using the pending_next linkage rather than the main TOC next linkage