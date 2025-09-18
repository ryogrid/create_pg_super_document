# DropObjectById

## Location
src/backend/catalog/dependency.c: 1189 - 1245

## Overview
DropObjectById is a static utility function that removes an object from its catalog table by OID, used as a low-level deletion mechanism for most PostgreSQL system catalogs when no special processing is required.

## Definition


## Detailed Description
DropObjectById provides a generic mechanism for deleting catalog entries by their object identifier. The function handles the low-level details of catalog tuple deletion, supporting both cached and non-cached lookup strategies. It first attempts to use the system cache if available for the object's catalog, falling back to a sequential scan approach when no cache exists. The function ensures proper locking by opening the target relation with RowExclusiveLock and maintains data consistency by using CatalogTupleDelete for the actual removal operation.

## Parameters / Member Variables
- : Pointer to an ObjectAddress structure containing:
  - : OID of the catalog relation containing the object
  - : OID of the specific object to be deleted
  - : Sub-object identifier (not used in this function)

## Dependencies
- Functions called/Symbols referenced:
  - get_object_catcache_oid: Retrieves cache ID for the object's catalog
  - table_open: Opens the catalog relation with specified lock
  - SearchSysCache1: Performs cached lookup of the target tuple
  - CatalogTupleDelete: Deletes the catalog tuple
  - ReleaseSysCache: Releases the cached tuple
  - ScanKeyInit: Initializes scan key for non-cached lookup
  - systable_beginscan: Begins system table scan
  - get_object_attnum_oid: Gets OID attribute number
  - get_object_oid_index: Gets OID index for the catalog
  - systable_getnext: Retrieves next tuple from scan
  - systable_endscan: Ends system table scan
  - table_close: Closes the catalog relation
  - get_object_class_descr: Gets human-readable class description for errors
- Called from:
  - doDeletion: Main deletion orchestration function

## Notes and Other Information
- This function is static and only used internally within the dependency.c module
- It implements a two-path deletion strategy: cached lookup for performance when available, sequential scan as fallback
- The function expects exactly one matching tuple and will error if the object is not found
- Proper error handling includes descriptive messages using get_object_class_descr
- Uses RowExclusiveLock to ensure safe concurrent access during deletion operations