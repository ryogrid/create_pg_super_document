# myLargeObjectExists

## Location
src/backend/storage/large_object/inv_api.c: 131 - 168

## Overview
Checks whether a large object with a given OID exists in the pg_largeobject_metadata catalog, using a specified snapshot for the visibility check.

## Definition


## Detailed Description
This internal function provides a snapshot-aware version of large object existence checking, similar to the LargeObjectExists() function in pg_largeobject.c but with the ability to specify a custom snapshot for visibility. It performs a system catalog scan on the pg_largeobject_metadata relation to search for a tuple with the specified large object OID. The function uses the system scan infrastructure with a B-tree equal strategy to efficiently locate the metadata record. It opens the metadata relation with AccessShareLock, performs the scan, and returns true if a valid tuple is found.

## Parameters / Member Variables
- : The OID of the large object to check for existence
- : The snapshot to use for visibility determination during the catalog scan

## Dependencies
- Functions called/Symbols referenced:
  - ScanKeyInit (to initialize scan key)
  - table_open (to open pg_largeobject_metadata relation)
  - systable_beginscan (to begin system catalog scan)
  - systable_getnext (to retrieve next tuple from scan)
  - systable_endscan (to end the scan)
  - table_close (to close the relation)
- Called from (representative examples):
  - inv_open

## Notes and Other Information
- Function is static (internal to inv_api.c)
- Uses AccessShareLock for reading the metadata relation
- Employs system catalog scanning infrastructure for efficient OID lookup
- Returns boolean result indicating existence
- Key difference from LargeObjectExists() is the configurable snapshot parameter
- Properly manages relation opening/closing and scan lifecycle