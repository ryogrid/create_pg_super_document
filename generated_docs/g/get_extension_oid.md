# get_extension_oid

## Location
[src/backend/commands/extension.c:145-189](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L145-L189)

## Overview
Looks up the Object Identifier (OID) for a PostgreSQL extension given its name, with optional error handling for missing extensions.

## Definition

```c
Oid
get_extension_oid(const char *extname, bool missing_ok)
```
## Detailed Description
This function performs a catalog lookup in the pg_extension system catalog to find the OID corresponding to a given extension name. It uses the system catalog scanning interface to search for the extension by name using the ExtensionNameIndexId index for efficient lookups. The function provides flexibility in error handling - it can either throw an error when an extension is not found or return InvalidOid based on the missing_ok parameter.

The function follows PostgreSQL's standard pattern for catalog lookups:
1. Opens the pg_extension system catalog with AccessShareLock
2. Initializes a scan key for the extension name using NAMEEQ operator
3. Performs an indexed scan using ExtensionNameIndexId
4. Extracts the OID from the found tuple or returns InvalidOid if not found
5. Properly cleans up resources by ending the scan and closing the relation

## Parameters / Member Variables
- : The name of the extension to look up (null-terminated C string)
- : Boolean flag controlling error behavior - if false, throws ERROR when extension not found; if true, returns InvalidOid silently

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md) (opens pg_extension catalog)
  - [ScanKeyInit](../S/ScanKeyInit.md) (initializes search key)
  - [systable_beginscan](../s/systable_beginscan.md) (starts catalog scan)
  - [systable_getnext](../s/systable_getnext.md) (retrieves next tuple)
  - [systable_endscan](../s/systable_endscan.md) (ends catalog scan)
  - [table_close](../t/table_close.md) (closes catalog relation)
  - [CStringGetDatum](../C/CStringGetDatum.md) (converts C string to Datum)
  - Form_pg_extension (cast to extension tuple structure)
  - ereport (error reporting)

- Called from (representative examples):
  - [CreateExtension](../C/CreateExtension.md) (during extension creation)
  - [get_required_extension](get_required_extension.md) (dependency resolution)
  - [AlterExtensionNamespace](../A/AlterExtensionNamespace.md) (namespace changes)
  - [get_object_address_unqualified](get_object_address_unqualified.md) (object addressing)
  - [binary_upgrade_create_empty_extension](../b/binary_upgrade_create_empty_extension.md) (pg_upgrade support)

## Notes and Other Information
- Assumes at most one matching tuple exists for any given extension name (extensions have unique names)
- Uses AccessShareLock to allow concurrent reads while preventing concurrent schema changes
- Part of PostgreSQL's extension management system introduced to support packaged extensions
- The function is declared in src/include/commands/extension.h and widely used throughout the extension management subsystem
- Returns InvalidOid (0) for non-existent extensions when missing_ok is true, following PostgreSQL conventions

## Simplified Source

```c
Oid get_extension_oid(const char *extname, bool missing_ok) {
    Oid result;

    // Open pg_extension catalog for reading
    Relation rel = table_open(ExtensionRelationId, AccessShareLock);

    // Setup scan key to search by extension name
    ScanKeyData entry[1];
    ScanKeyInit(&entry[0], Anum_pg_extension_extname, BTEqualStrategyNumber,
                F_NAMEEQ, CStringGetDatum(extname));

    // Perform indexed scan using extension name index
    SysScanDesc scandesc = systable_beginscan(rel, ExtensionNameIndexId,
                                             true, NULL, 1, entry);

    HeapTuple tuple = systable_getnext(scandesc);

    // Extract OID from tuple if found
    if (HeapTupleIsValid(tuple))
        result = ((Form_pg_extension) GETSTRUCT(tuple))->oid;
    else
        result = InvalidOid;

    // Clean up resources
    systable_endscan(scandesc);
    table_close(rel, AccessShareLock);

    // Handle missing extension based on missing_ok flag
    if (!OidIsValid(result) && !missing_ok)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("extension \"%s\" does not exist", extname)));

    return result;
}
```