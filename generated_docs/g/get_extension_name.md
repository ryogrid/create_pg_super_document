# get_extension_name

## Location
[src/backend/commands/extension.c:190-228](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L190-L228)

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
  - [table_open](../t/table_open.md) (opens pg_extension catalog)
  - [ScanKeyInit](../S/ScanKeyInit.md) (initializes search key)  
  - [systable_beginscan](../s/systable_beginscan.md) (starts catalog scan)
  - [systable_getnext](../s/systable_getnext.md) (retrieves next tuple)
  - [systable_endscan](../s/systable_endscan.md) (ends catalog scan)
  - [table_close](../t/table_close.md) (closes catalog relation)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (converts OID to Datum)
  - Form_pg_extension (cast to extension tuple structure)
  - [pstrdup](../p/pstrdup.md) (duplicate string with palloc)
  - NameStr (extracts string from Name type)

- Called from (representative examples):
  - [getObjectDescription](getObjectDescription.md) (object description generation)
  - [getObjectIdentityParts](getObjectIdentityParts.md) (object identity formatting)
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md) (dependency tracking)
  - [checkMembershipInCurrentExtension](../c/checkMembershipInCurrentExtension.md) (membership validation)
  - [RemoveExtensionById](../R/RemoveExtensionById.md) (extension removal)
  - [AlterExtensionNamespace](../A/AlterExtensionNamespace.md) (namespace operations)

## Notes and Other Information
- Returns a palloc'd string that must be freed by the caller when no longer needed
- Assumes at most one matching tuple exists for any given extension OID (OIDs are unique)
- Uses AccessShareLock to allow concurrent reads while preventing concurrent schema changes
- Returns NULL instead of throwing an error when the extension OID is not found
- The returned string is allocated in the current memory context using PostgreSQL's memory management system
- Part of PostgreSQL's extension management system and widely used for error reporting, logging, and object identification
- Complementary function to get_extension_oid, providing bidirectional name/OID mapping for extensions

## Simplified Source

```c
char *get_extension_name(Oid ext_oid)
{
    char *result;
    Relation rel;
    SysScanDesc scandesc;
    HeapTuple tuple;
    ScanKeyData entry[1];

    // Open the pg_extension system catalog
    rel = table_open(ExtensionRelationId, AccessShareLock);

    // Set up scan key for extension OID
    ScanKeyInit(&entry[0], Anum_pg_extension_oid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(ext_oid));

    // Begin indexed scan using ExtensionOidIndexId
    scandesc = systable_beginscan(rel, ExtensionOidIndexId, true, NULL, 1, entry);

    // Get the matching tuple
    tuple = systable_getnext(scandesc);

    // Extract extension name if found, otherwise return NULL
    if (HeapTupleIsValid(tuple))
        result = pstrdup(NameStr(((Form_pg_extension) GETSTRUCT(tuple))->extname));
    else
        result = NULL;

    // Clean up
    systable_endscan(scandesc);
    table_close(rel, AccessShareLock);

    return result;  // Caller must pfree() this if not NULL
}
```