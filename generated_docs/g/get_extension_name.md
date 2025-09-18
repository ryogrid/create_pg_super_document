# get_extension_name

## Location
src/backend/commands/extension.c: 190 - 228

## Overview
Performs a reverse lookup to retrieve the name of a PostgreSQL extension given its Object Identifier (OID).

## Definition
```c
char *get_extension_name(Oid ext_oid)
```

## Detailed Description
This function performs a catalog lookup in the pg_extension system catalog to find the extension name corresponding to a given extension OID. It uses the system catalog scanning interface to search for the extension by OID using the ExtensionOidIndexId index for efficient lookups. The function returns a newly allocated string containing the extension name, or NULL if the extension does not exist.

The function follows PostgreSQL's standard pattern for catalog lookups:
1. Opens the pg_extension system catalog with AccessShareLock
2. Initializes a scan key for the extension OID using OIDEQ operator  
3. Performs an indexed scan using ExtensionOidIndexId
4. Extracts the extension name from the found tuple using pstrdup for memory allocation
5. Properly cleans up resources by ending the scan and closing the relation

Unlike get_extension_oid, this function does not have a missing_ok parameter and simply returns NULL for non-existent extensions.

## Parameters / Member Variables
- `ext_oid`: The Object Identifier of the extension whose name should be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - table_open (opens pg_extension catalog)
  - ScanKeyInit (initializes search key)  
  - systable_beginscan (starts catalog scan)
  - systable_getnext (retrieves next tuple)
  - systable_endscan (ends catalog scan)
  - table_close (closes catalog relation)
  - ObjectIdGetDatum (converts OID to Datum)
  - Form_pg_extension (cast to extension tuple structure)
  - pstrdup (duplicate string with palloc)
  - NameStr (extracts string from Name type)

- Called from (representative examples):
  - getObjectDescription (object description generation)
  - getObjectIdentityParts (object identity formatting)
  - recordDependencyOnCurrentExtension (dependency tracking)
  - checkMembershipInCurrentExtension (membership validation)
  - RemoveExtensionById (extension removal)
  - AlterExtensionNamespace (namespace operations)

## Notes and Other Information
- Returns a palloc'd string that must be freed by the caller when no longer needed
- Assumes at most one matching tuple exists for any given extension OID (OIDs are unique)
- Uses AccessShareLock to allow concurrent reads while preventing concurrent schema changes
- Returns NULL instead of throwing an error when the extension OID is not found
- The returned string is allocated in the current memory context using PostgreSQL's memory management system
- Part of PostgreSQL's extension management system and widely used for error reporting, logging, and object identification
- Complementary function to get_extension_oid, providing bidirectional name/OID mapping for extensions