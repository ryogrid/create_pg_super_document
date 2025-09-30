# get_extension_schema

## Location
[src/backend/commands/extension.c:229-265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L229-L265)

## Overview
Retrieves the schema (namespace) OID where a PostgreSQL extension is installed, given the extension's OID.

## Definition
```c
Oid get_extension_schema(Oid ext_oid)
```

## Detailed Description
This function performs a catalog lookup in the pg_extension system catalog to find the schema (namespace) OID where the specified extension is installed. Extensions in PostgreSQL are installed into specific schemas, and this function provides access to that schema information by looking up the extnamespace field in the pg_extension catalog.

The function follows PostgreSQL's standard pattern for catalog lookups:
1. Opens the pg_extension system catalog with AccessShareLock
2. Initializes a scan key for the extension OID using OIDEQ operator
3. Performs an indexed scan using ExtensionOidIndexId 
4. Extracts the extnamespace field from the found tuple
5. Properly cleans up resources by ending the scan and closing the relation

This function is essential for extension management operations that need to understand or modify the schema context of an extension.

## Parameters / Member Variables
- `ext_oid`: The Object Identifier of the extension whose schema should be retrieved

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

- Called from (representative examples):
  - [CreateExtensionInternal](../C/CreateExtensionInternal.md) (during extension creation)
  - [ApplyExtensionUpdates](../A/ApplyExtensionUpdates.md) (during extension updates)
  - [ExecAlterExtensionContentsRecurse](../E/ExecAlterExtensionContentsRecurse.md) (content management operations)

## Notes and Other Information
- Returns InvalidOid (0) if the extension OID does not exist, rather than throwing an error
- Assumes at most one matching tuple exists for any given extension OID (OIDs are unique)
- Uses AccessShareLock to allow concurrent reads while preventing concurrent schema changes
- The returned schema OID can be used with other PostgreSQL functions to get the schema name or perform schema-related operations
- Essential for operations that need to understand the namespace context of extension objects
- Part of PostgreSQL's extension management system and used internally by extension creation and maintenance functions
- The extnamespace field in pg_extension corresponds to the schema where the extension's objects are created

## Simplified Source

```c
Oid get_extension_schema(Oid ext_oid) {
    Oid result;
    Relation rel;
    SysScanDesc scandesc;
    HeapTuple tuple;
    ScanKeyData entry[1];

    // Open pg_extension catalog with shared lock
    rel = table_open(ExtensionRelationId, AccessShareLock);

    // Set up scan key to find extension by OID
    ScanKeyInit(&entry[0], Anum_pg_extension_oid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(ext_oid));

    // Start indexed scan
    scandesc = systable_beginscan(rel, ExtensionOidIndexId, true,
                                  NULL, 1, entry);

    // Get the tuple (should be at most one)
    tuple = systable_getnext(scandesc);

    // Extract schema OID if extension exists
    if (HeapTupleIsValid(tuple)) {
        result = ((Form_pg_extension) GETSTRUCT(tuple))->extnamespace;
    } else {
        result = InvalidOid;  // Extension not found
    }

    // Clean up
    systable_endscan(scandesc);
    table_close(rel, AccessShareLock);

    return result;
}
```