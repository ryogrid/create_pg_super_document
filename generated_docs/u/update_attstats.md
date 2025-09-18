# update_attstats

## Location
src/backend/commands/analyze.c: 1609 - 1751

## Overview
Updates attribute statistics in the pg_statistic catalog table by inserting new or replacing existing statistical data computed during table analysis.

## Definition


## Detailed Description
The update_attstats function persists computed statistics for table columns to the pg_statistic system catalog. It processes an array of VacAttrStats structures containing statistical data collected during table analysis and either inserts new pg_statistic rows or updates existing ones.

For each valid attribute statistic, the function constructs a complete pg_statistic tuple containing the relation OID, attribute number, inheritance flag, null fraction, average width, distinct value estimate, and up to STATISTIC_NUM_SLOTS worth of detailed statistics (kinds, operators, collations, numeric arrays, and value arrays).

The function handles both regular table statistics and inheritance tree statistics (when inh=true). It uses the system cache to check for existing statistics rows and performs appropriate INSERT or UPDATE operations through the catalog tuple management functions.

## Parameters / Member Variables
- : OID of the relation whose statistics are being updated
- : Boolean indicating whether these are inheritance tree statistics
- : Number of attributes in the vacattrstats array
- : Array of pointers to VacAttrStats structures containing computed statistics

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - SearchSysCache3
  - CatalogOpenIndexes
  - heap_modify_tuple
  - heap_form_tuple
  - CatalogTupleUpdateWithInfo
  - CatalogTupleInsertWithInfo
  - construct_array_builtin
  - construct_array
  - heap_freetuple
  - CatalogCloseIndexes
- Called from (representative examples):
  - do_analyze_rel (called twice, once for regular stats and once for inheritance stats)

## Notes and Other Information
- Only processes attributes where stats_valid is true in the VacAttrStats structure
- Constructs complete pg_statistic tuples with all STATISTIC_NUM_SLOTS filled appropriately
- Uses catalog index state management for efficient bulk operations
- Handles both numeric arrays (stanumbers) and value arrays (stavalues) with proper type information
- Skips processing if natts <= 0 to handle empty attribute lists
- Takes RowExclusiveLock on the pg_statistic relation during updates
- Does not compute statistics for pg_statistic itself (avoided by analyze_rel logic)