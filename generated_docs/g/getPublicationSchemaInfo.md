# getPublicationSchemaInfo

## Location
src/backend/catalog/objectaddress.c: 2855 - 2902

## Overview
Retrieves publication name and schema name information from a publication schema object address, returning palloc'd strings for both names.

## Definition


## Detailed Description
This static function extracts publication and namespace information from a publication schema object by performing lookups in the system catalogs. It first searches the pg_publication_namespace catalog using the object's OID to retrieve the publication schema record, then uses the stored publication ID and namespace ID to fetch the corresponding names.

The function handles error conditions gracefully based on the missing_ok parameter. When missing_ok is false, it logs errors for missing publications or schemas. When true, it returns false without logging errors. The function properly manages memory by freeing allocated publication names if namespace lookup fails, and always releases system cache tuples.

Both returned strings (pubname and nspname) are palloc'd and must be freed by the caller to avoid memory leaks.

## Parameters / Member Variables
- `object`: Pointer to ObjectAddress containing the publication schema object ID
- `missing_ok`: Boolean flag controlling error handling behavior (true = return false on missing objects, false = log errors)
- `pubname`: Output parameter for publication name (palloc'd string, caller must free)
- `nspname`: Output parameter for schema/namespace name (palloc'd string, caller must free)

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_publication_namespace (catalog form structure)
  - [SearchSysCache1](../S/SearchSysCache1.md), ReleaseSysCache (system cache operations)
  - [get_publication_name](get_publication_name.md) (publication name lookup)
  - [get_namespace_name](get_namespace_name.md) (namespace name lookup)  
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md), HeapTupleIsValid, GETSTRUCT (tuple handling)
  - elog, ERROR (error reporting)
  - [pfree](../p/pfree.md) (memory management)
- Called from (representative examples):
  - [getObjectDescription](getObjectDescription.md)
  - [getObjectIdentityParts](getObjectIdentityParts.md)

## Notes and Other Information
- Static function internal to objectaddress.c for publication schema object handling
- Handles both error-throwing and error-returning modes via missing_ok parameter
- Properly manages memory allocation and cleanup for returned strings
- Uses PUBLICATIONNAMESPACE syscache for efficient lookups
- Located in src/backend/catalog/objectaddress.c:2855-2902
- Part of PostgreSQL's logical replication publication schema management system
- Returns false on failure, true on success with valid string pointers