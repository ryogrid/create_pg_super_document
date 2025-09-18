# enum_endpoint

## Location
src/backend/utils/adt/enum.c: 392 - 436

## Overview
A static helper function that implements common code for finding the first or last member of an enum type by scanning the pg_enum system catalog in a specified direction.

## Definition


## Detailed Description
The  function serves as the core implementation for both  and  SQL functions. It performs an ordered scan of the pg_enum system catalog to find either the first or last enum value based on the specified scan direction. The function uses the  to ensure proper ordering and explicitly avoids the system cache for safety reasons related to concurrent enum modifications.

The function implements proper transaction safety by calling  to ensure that uncommitted enum values are not used in SQL operations, preventing potential index corruption during transaction rollbacks.

## Parameters / Member Variables
- : The OID of the enum type for which to find the endpoint value
- : The scan direction (ForwardScanDirection for first, BackwardScanDirection for last)

## Dependencies
- Functions called/Symbols referenced:
  - ScanKeyInit
  - table_open
  - index_open
  - systable_beginscan_ordered
  - systable_getnext_ordered
  - check_safe_enum_use
  - systable_endscan_ordered
  - index_close
  - table_close
- Called from:
  - enum_first
  - enum_last

## Notes and Other Information
- This is a static function, not directly accessible outside the enum.c module
- Explicitly avoids using the system cache due to concurrency concerns with enum renumbering operations
- Returns InvalidOid for empty enum types
- Uses ordered scanning with the pg_enum_typid_sortorder_index for consistent results
- Implements proper resource cleanup by closing relations and indexes after use