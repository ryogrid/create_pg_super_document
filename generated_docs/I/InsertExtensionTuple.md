# InsertExtensionTuple

## Location
[src/backend/commands/extension.c:1866-1953](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L1866-L1953)

## Overview
InsertExtensionTuple creates a new pg_extension catalog tuple and establishes all necessary dependency relationships for the extension, including owner, schema, and prerequisite extensions.

## Definition

```c
ObjectAddress
InsertExtensionTuple(const char *extName, Oid extOwner,
					 Oid schemaOid, bool relocatable, const char *extVersion,
					 Datum extConfig, Datum extCondition,
					 List *requiredExtensions)
```
## Detailed Description
This function performs the core catalog operations for extension registration. It creates a new tuple in the pg_extension system catalog with all the extension metadata, generates a unique OID for the extension, and establishes dependency relationships. The function handles both required and optional extension configuration arrays (extConfig and extCondition), records dependencies on the extension owner, target schema, and all prerequisite extensions, and invokes post-creation hooks. It's specifically designed to be usable by pg_upgrade, which needs to create extension entries without running installation scripts.

## Parameters / Member Variables
- `*extName`: Name of the extension to register
- `extOwner`: OID of the user who owns the extension
- `schemaOid`: OID of the schema where the extension is installed
- `relocatable`: Boolean flag indicating if the extension can be moved between schemas
- `*extVersion`: Version string of the extension being installed
- `extConfig`: Configuration array (tables/views) or NULL pointer as Datum
- `extCondition`: Condition array (WHERE clauses for config tables) or NULL pointer as Datum
- `*requiredExtensions`: List of OIDs of extensions that this extension depends on
## Dependencies
- Functions called/Symbols referenced:
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - DirectFunctionCall1
  - [namein](../n/namein.md)
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md)
  - [new_object_addresses](../n/new_object_addresses.md)
  - ObjectAddressSet
  - [add_exact_object_address](../a/add_exact_object_address.md)
  - [record_object_address_dependencies](../r/record_object_address_dependencies.md)
  - [free_object_addresses](../f/free_object_addresses.md)
  - InvokeObjectPostCreateHook
- Called from (representative examples):
  - [CreateExtensionInternal](../C/CreateExtensionInternal.md)
  - [binary_upgrade_create_empty_extension](../b/binary_upgrade_create_empty_extension.md)

## Notes and Other Information
- This function is exported specifically for pg_upgrade support, allowing extension registration without script execution
- Uses RowExclusiveLock on pg_extension catalog during tuple insertion
- Handles nullable extConfig and extCondition fields properly using PointerGetDatum(NULL) checks
- Records DEPENDENCY_NORMAL relationships to owner, schema, and prerequisite extensions
- Implements proper memory management with heap_freetuple and free_object_addresses cleanup
- Invokes object creation hooks for extension registration events
- Returns ObjectAddress for the newly created extension for further processing

## Simplified Source

```c
ObjectAddress InsertExtensionTuple(const char *extName, Oid extOwner,
                                  Oid schemaOid, bool relocatable,
                                  const char *extVersion, Datum extConfig,
                                  Datum extCondition, List *requiredExtensions) {
    Oid extensionOid;
    Relation rel;
    Datum values[Natts_pg_extension];
    bool nulls[Natts_pg_extension];
    HeapTuple tuple;
    ObjectAddress myself;
    ObjectAddresses *refobjs;

    // Open pg_extension catalog for insertion
    rel = table_open(ExtensionRelationId, RowExclusiveLock);

    // Initialize tuple data
    memset(values, 0, sizeof(values));
    memset(nulls, 0, sizeof(nulls));

    // Generate new OID and set basic fields
    extensionOid = GetNewOidWithIndex(rel, ExtensionOidIndexId, Anum_pg_extension_oid);
    values[Anum_pg_extension_oid - 1] = ObjectIdGetDatum(extensionOid);
    values[Anum_pg_extension_extname - 1] = DirectFunctionCall1(namein, CStringGetDatum(extName));
    values[Anum_pg_extension_extowner - 1] = ObjectIdGetDatum(extOwner);
    values[Anum_pg_extension_extnamespace - 1] = ObjectIdGetDatum(schemaOid);
    values[Anum_pg_extension_extrelocatable - 1] = BoolGetDatum(relocatable);
    values[Anum_pg_extension_extversion - 1] = CStringGetTextDatum(extVersion);

    // Handle optional config and condition arrays
    if (extConfig == PointerGetDatum(NULL)) {
        nulls[Anum_pg_extension_extconfig - 1] = true;
    } else {
        values[Anum_pg_extension_extconfig - 1] = extConfig;
    }

    if (extCondition == PointerGetDatum(NULL)) {
        nulls[Anum_pg_extension_extcondition - 1] = true;
    } else {
        values[Anum_pg_extension_extcondition - 1] = extCondition;
    }

    // Create and insert tuple
    tuple = heap_form_tuple(rel->rd_att, values, nulls);
    CatalogTupleInsert(rel, tuple);
    heap_freetuple(tuple);
    table_close(rel, RowExclusiveLock);

    // Record dependencies
    recordDependencyOnOwner(ExtensionRelationId, extensionOid, extOwner);

    // Build dependency list for schema and required extensions
    refobjs = new_object_addresses();
    ObjectAddressSet(myself, ExtensionRelationId, extensionOid);

    // Add schema dependency
    ObjectAddress nsp;
    ObjectAddressSet(nsp, NamespaceRelationId, schemaOid);
    add_exact_object_address(&nsp, refobjs);

    // Add required extension dependencies
    foreach(lc, requiredExtensions) {
        Oid reqext = lfirst_oid(lc);
        ObjectAddress otherext;
        ObjectAddressSet(otherext, ExtensionRelationId, reqext);
        add_exact_object_address(&otherext, refobjs);
    }

    // Record all dependencies and cleanup
    record_object_address_dependencies(&myself, refobjs, DEPENDENCY_NORMAL);
    free_object_addresses(refobjs);

    // Invoke post-creation hook
    InvokeObjectPostCreateHook(ExtensionRelationId, extensionOid, 0);

    return myself;
}
```