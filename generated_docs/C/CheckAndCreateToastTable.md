# CheckAndCreateToastTable

## Location
src/backend/catalog/toasting.c: 78 - 97

## Overview
CheckAndCreateToastTable is a static function that serves as the common implementation backend for all TOAST table creation variants, handling relation opening, delegation to create_toast_table, and cleanup.

## Definition


## Detailed Description
This is the core implementation function that underlies all the public TOAST table creation functions. It provides a unified interface that handles the common pattern of opening a relation, calling create_toast_table to do the actual work, and then properly closing the relation. The function acts as an adapter between the public API functions and the lower-level create_toast_table implementation.

The function takes care of proper resource management by opening the relation with the specified lock mode and ensuring it's properly closed afterward. It passes through all necessary parameters to create_toast_table, including reloptions for customizing the TOAST table, lock mode for concurrency control, check parameter for validation behavior, and old TOAST OID for operations that need to reference previous TOAST tables.

## Parameters / Member Variables
- : The OID of the relation for which to potentially create a TOAST table
- : Datum containing reloptions for the TOAST table configuration
- : The lock mode to use when accessing the relation
- : Boolean flag controlling validation behavior (true for ALTER TABLE scenarios, false for new relation scenarios)
- : The OID of an existing TOAST table, if any (used for table rebuilding operations)

## Dependencies
- Functions called/Symbols referenced:
  - create_toast_table
  - table_open
  - table_close
- Called from (representative examples):
  - AlterTableCreateToastTable (in src/backend/catalog/toasting.c:60)
  - NewHeapCreateToastTable (in src/backend/catalog/toasting.c:67)
  - NewRelationCreateToastTable (in src/backend/catalog/toasting.c:73)

## Notes and Other Information
- This is a static function, not part of the public API - it serves as an implementation detail
- Handles proper resource management by opening and closing relations with appropriate locking
- Acts as a bridge between the public API functions and the core create_toast_table implementation
- The InvalidOid parameters passed to create_toast_table indicate that TOAST table and index OIDs should be automatically assigned
- The NoLock parameter for table_close indicates the lock should be retained as acquired during table_open