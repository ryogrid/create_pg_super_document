# autovacuum_do_vac_analyze

## Location
src/backend/postmaster/autovacuum.c: 3118 - 3157

## Overview
Executes vacuum and/or analyze operations on a specified table within the autovacuum framework, handling memory context management and reporting.

## Definition


## Detailed Description
This function serves as the execution layer for autovacuum operations after decision-making is complete. It performs the following key operations:

1. **Activity Reporting**: Updates pgstat to indicate what autovacuum operation is being performed
2. **Memory Context Management**: Creates a dedicated memory context for vacuum operations that persists across transaction boundaries
3. **Target Preparation**: Constructs the necessary data structures (RangeVar, VacuumRelation) to identify the target table
4. **Operation Execution**: Calls the main vacuum() function with appropriate parameters
5. **Cleanup**: Properly destroys the vacuum memory context after completion

The function bridges the gap between autovacuum's table selection logic and PostgreSQL's core vacuum implementation, ensuring proper resource management and transaction handling.

## Parameters / Member Variables
- : Autovacuum table structure containing table identification (namespace, relation name, OID) and vacuum parameters (at_params)
- : Buffer access strategy to control I/O behavior during vacuum operations

## Dependencies
- Functions called/Symbols referenced:
  - autovac_report_activity
  - AllocSetContextCreate
  - [makeRangeVar](../m/makeRangeVar.md)
  - [makeVacuumRelation](../m/makeVacuumRelation.md)
  - vacuum
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
- Called from (representative examples):
  - [do_autovacuum](../d/do_autovacuum.md)

## Notes and Other Information
- Assumes caller has switched to a memory context that survives transaction commit
- Creates a dedicated 'Vacuum' memory context for cross-transaction storage requirements
- Uses OID-based table identification for vacuum operations
- The function is designed to be called within the autovacuum worker process context
- Handles both vacuum and analyze operations based on parameters in the tab->at_params structure