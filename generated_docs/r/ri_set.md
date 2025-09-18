# ri_set

## Location
src/backend/utils/adt/ri_triggers.c: 1031 - 1225

## Overview
This is the core implementation function that handles SET NULL and SET DEFAULT actions for foreign key constraints on both DELETE and UPDATE operations.

## Definition


## Detailed Description
ri_set is the central workhorse function that implements the actual logic for ON DELETE SET NULL, ON DELETE SET DEFAULT, ON UPDATE SET NULL, and ON UPDATE SET DEFAULT foreign key constraint actions. It dynamically builds and executes SQL UPDATE statements to modify foreign key values in the referencing table when the referenced primary key is deleted or updated. The function handles query plan caching, column-specific updates based on constraint configuration, and ensures referential integrity through validation checks.

## Parameters / Member Variables
- : TriggerData structure containing trigger context information including relation references and tuple data
- : Boolean flag indicating whether to set values to NULL (true) or DEFAULT (false)
- : Integer specifying the trigger type (RI_TRIGTYPE_DELETE or RI_TRIGTYPE_UPDATE)

## Dependencies
- Functions called/Symbols referenced:
  - ri_FetchConstraintInfo (retrieves constraint metadata)
  - table_open (opens the foreign key relation with RowExclusiveLock)
  - SPI_connect/SPI_finish (SPI interface management)
  - ri_BuildQueryKey/ri_FetchPreparedPlan (query plan management)
  - ri_PlanCheck (prepares new query plans when needed)
  - ri_PerformCheck (executes the UPDATE statement)
  - ri_restrict (performs additional validation for SET DEFAULT case)
  - Various utility functions: RIAttName, RIAttType, RIAttCollation, quoteRelationName, quoteOneName
- Called from (representative examples):
  - RI_FKey_setnull_del
  - RI_FKey_setnull_upd
  - RI_FKey_setdefault_del
  - RI_FKey_setdefault_upd

## Notes and Other Information
- The function supports both full and partial column updates based on confdelsetcols configuration
- Query plans are cached for performance using the RI_QueryKey mechanism
- Handles partitioned tables by omitting the ONLY keyword when appropriate
- For SET DEFAULT operations, performs additional validation via ri_restrict to ensure no constraint violations
- Uses SPI (Server Programming Interface) to execute dynamically constructed UPDATE statements
- Located in src/backend/utils/adt/ri_triggers.c:1031-1225