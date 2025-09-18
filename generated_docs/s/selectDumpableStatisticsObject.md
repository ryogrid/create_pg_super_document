# selectDumpableStatisticsObject

## Location
src/bin/pg_dump/pg_dump.c: 2126 - 2143

## Overview
Determines whether an extended statistics object should be dumped based on the dumpability of its associated schema and table.

## Definition


## Detailed Description
This function implements the policy-setting logic for extended statistics objects in pg_dump. An extended statistics object is marked for dumping only if both its containing schema and the table it operates on are being dumped. The function first checks for extension membership (which overrides all other policies), then evaluates the namespace (schema) dump status, and finally verifies that the associated table is being dumped with its definition.

The current implementation assumes statistics objects operate on a single table, with a note that cross-table statistics would require additional consideration in the future.

## Parameters / Member Variables
- : Pointer to the StatsExtInfo structure representing the extended statistics object to be evaluated
- : Pointer to the Archive structure containing dump options and configuration

## Dependencies
- Functions called/Symbols referenced:
  - checkExtensionMembership
  - DUMP_COMPONENT_DEFINITION
  - DUMP_COMPONENT_NONE
  - StatsExtInfo (struct)
- Called from (representative examples):
  - getExtendedStatistics

## Notes and Other Information
- Extended statistics objects are only dumped when both their schema and table are being dumped
- Extension membership takes precedence over all other dumping policies
- The function assumes single-table statistics; cross-table statistics would require design changes
- The statistics object inherits the dump_contains flag from its namespace initially
- If the associated table is NULL or not being dumped with definition, the statistics object is marked as not dumpable