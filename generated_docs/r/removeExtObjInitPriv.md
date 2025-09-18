# removeExtObjInitPriv

## Location
src/backend/catalog/aclchk.c: 4573 - 4655

## Overview
Removes all initial privilege entries for a database object and its sub-objects from pg_init_privs when the object is dropped from an extension via ALTER EXTENSION DROP.

## Definition


## Detailed Description
This function serves as the counterpart to recordExtObjInitPriv(), handling the cleanup of pg_init_privs entries when objects are removed from extensions. It systematically removes privilege records for both the main object and any sub-objects (such as columns for relations) by calling recordExtensionInitPrivWorker() with a NULL ACL parameter, which signals deletion.

The function handles relations specially by iterating through all columns (including dropped ones) to ensure complete cleanup of column-level privilege records. Unlike recordExtObjInitPriv(), this function removes records for dropped columns as well, ensuring thorough cleanup when objects are removed from extensions.

The function follows the same object type logic as its recording counterpart, skipping objects that don't have permissions (indexes, partitioned indexes, composite types) but processing relations with potential column-level privileges differently from simple sequences.

## Parameters / Member Variables
- : OID of the database object whose privilege entries should be removed from pg_init_privs
- : OID of the system catalog class containing the object (e.g., RelationRelationId for tables)

## Dependencies
- Functions called/Symbols referenced:
  - recordExtensionInitPrivWorker (worker function called with NULL ACL to delete entries)
  - SearchSysCache1, SearchSysCache2 (system catalog lookups)
  - HeapTupleIsValid (validates tuple existence)
  - ReleaseSysCache (releases system cache references)
  - ObjectIdGetDatum, Int16GetDatum (datum conversion functions)
- Called from:
  - ExecAlterExtensionContentsRecurse (during ALTER EXTENSION DROP operations)

## Notes and Other Information
- Counterpart function to recordExtObjInitPriv() for extension cleanup
- Part of PostgreSQL's extension privilege management system
- Removes entries from pg_init_privs by calling recordExtensionInitPrivWorker() with NULL ACL
- Handles column-level privilege cleanup for relations, including dropped columns
- Skips objects without permissions (indexes, composite types) similar to the recording function
- Ensures complete cleanup when objects are removed from extensions
- Critical for maintaining pg_init_privs consistency during extension operations
- Unlike recording, removal processes even dropped columns to ensure complete cleanup
- The NULL ACL parameter to recordExtensionInitPrivWorker() triggers deletion logic
- Used during ALTER EXTENSION DROP to clean up privilege tracking entries