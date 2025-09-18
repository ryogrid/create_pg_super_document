# AlterStatistics

## Location
src/backend/commands/statscmds.c: 599 - 721

## Overview
Modifies the statistics target of an existing PostgreSQL extended statistics object, controlling how much sample data is collected during ANALYZE operations.

## Definition


## Detailed Description
This function implements the ALTER STATISTICS SQL command, specifically handling changes to the statistics target parameter. The statistics target determines the sample size used when collecting extended statistics during ANALYZE operations - higher values provide more accurate statistics but require more storage and computation time.

The function validates the new target value (must be between 0 and MAX_STATISTICS_TARGET), checks object ownership permissions, and updates the stxstattarget column in the pg_statistic_ext system catalog. It supports the IF EXISTS clause to gracefully handle non-existent statistics objects.

Key validation steps include:
- Range checking the statistics target value
- Verifying object existence and ownership  
- Proper handling of default values (NULL in catalog)
- Warning when target exceeds maximum and auto-clamping to MAX_STATISTICS_TARGET

## Parameters / Member Variables
- : AlterStatsStmt structure containing the statistics object name, new target value, and IF EXISTS flag from the parsed ALTER STATISTICS command

## Dependencies
- Functions called/Symbols referenced:
  - [get_statistics_object_oid](../g/get_statistics_object_oid.md) (resolves statistics object name to OID)
  - [object_ownercheck](../o/object_ownercheck.md) (verifies ownership permissions)  
  - [heap_modify_tuple](../h/heap_modify_tuple.md) (creates updated catalog tuple)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (commits changes to pg_statistic_ext)
  - InvokeObjectPostAlterHook (triggers post-alter event hooks)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (src/backend/tcop/utility.c:1907)

## Notes and Other Information
- Only supports altering the statistics target, not other statistics object properties
- Target value of -1 (from previous PostgreSQL versions) is treated as default
- Default target uses NULL value in catalog (inherits from default_statistics_target GUC)
- Values above MAX_STATISTICS_TARGET are automatically clamped with a warning
- No dependency updates needed since only the target value changes
- Returns InvalidObjectAddress when IF EXISTS is used and object doesn't exist
- Requires RowExclusiveLock on StatisticExtRelationId catalog table