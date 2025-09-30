# TypeShellMake

## Location
[src/backend/catalog/pg_type.c:57-194](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_type.c#L57-L194)

## Overview
TypeShellMake creates a "shell" type tuple in the pg_type system catalog with placeholder values, allowing I/O functions to reference the type before its full definition is completed during type creation.

## Definition

```c
ObjectAddress
TypeShellMake(const char *typeName, Oid typeNamespace, Oid ownerId)
```
## Detailed Description
TypeShellMake is a critical function in PostgreSQL's type system that creates an incomplete "shell" type entry in the pg_type catalog. This shell type serves as a placeholder during the type creation process, particularly important for handling forward references and circular dependencies between types.

The function creates a type tuple with dummy but consistent values (modeled after int4 characteristics) and marks the type as undefined by setting  to false. It uses  as the type category to prevent the shell type from being mistaken for a usable type. The shell type uses special I/O functions (F_SHELL_IN and F_SHELL_OUT) that are designed to handle incomplete types.

Once the full CREATE TYPE command is processed, the dummy values in the shell type are replaced with the actual type specifications, and  is set to true to indicate the type is fully defined and ready for use.

## Parameters
- : The name of the type to create the shell for
- : The OID of the namespace (schema) where the type will be created  
- : The OID of the user who owns the type

## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid
  - [table_open](../t/table_open.md)
  - [namestrcpy](../n/namestrcpy.md)
  - [NameGetDatum](../N/NameGetDatum.md), Int16GetDatum, CharGetDatum, BoolGetDatum, ObjectIdGetDatum, Int32GetDatum
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - IsBootstrapProcessingMode
  - [GenerateTypeDependencies](../G/GenerateTypeDependencies.md)
  - InvokeObjectPostCreateHook
  - ObjectAddressSet
  - [heap_freetuple](../h/heap_freetuple.md)
  - [table_close](../t/table_close.md)
- Called from (representative examples):
  - [compute_return_type](../c/compute_return_type.md) (src/backend/commands/functioncmds.c:153)
  - [DefineType](../D/DefineType.md) (src/backend/commands/typecmds.c:267)

## Notes and Other Information
- The shell type is created with characteristics similar to int4 (4-byte length, pass-by-value, integer alignment)
- Uses special shell I/O functions (F_SHELL_IN/F_SHELL_OUT) that handle incomplete type references
- Supports binary upgrade mode by using predetermined OIDs when  is set
- Dependencies are only created when not in bootstrap processing mode
- The function returns an ObjectAddress pointing to the newly created shell type
- Critical for handling circular type dependencies and forward references in complex type hierarchies

## Simplified Source

```c
ObjectAddress
TypeShellMake(const char *typeName, Oid typeNamespace, Oid ownerId)
{
    Relation pg_type_desc;
    TupleDesc tupDesc;
    HeapTuple tup;
    Datum values[Natts_pg_type];
    bool nulls[Natts_pg_type];
    Oid typoid;
    NameData name;
    ObjectAddress address;

    // Open pg_type catalog table
    pg_type_desc = table_open(TypeRelationId, RowExclusiveLock);
    tupDesc = pg_type_desc->rd_att;

    // Initialize arrays for tuple creation
    for (int i = 0; i < Natts_pg_type; ++i) {
        nulls[i] = false;
        values[i] = (Datum) NULL;
    }

    // Set basic type information with dummy but consistent values (like int4)
    namestrcpy(&name, typeName);
    values[Anum_pg_type_typname - 1] = NameGetDatum(&name);
    values[Anum_pg_type_typnamespace - 1] = ObjectIdGetDatum(typeNamespace);
    values[Anum_pg_type_typowner - 1] = ObjectIdGetDatum(ownerId);
    values[Anum_pg_type_typlen - 1] = Int16GetDatum(sizeof(int32));
    values[Anum_pg_type_typbyval - 1] = BoolGetDatum(true);
    values[Anum_pg_type_typtype - 1] = CharGetDatum(TYPTYPE_PSEUDO);
    values[Anum_pg_type_typcategory - 1] = CharGetDatum(TYPCATEGORY_PSEUDOTYPE);
    values[Anum_pg_type_typispreferred - 1] = BoolGetDatum(false);
    values[Anum_pg_type_typisdefined - 1] = BoolGetDatum(false);  // Mark as shell type
    values[Anum_pg_type_typdelim - 1] = CharGetDatum(DEFAULT_TYPDELIM);

    // Set dummy OIDs for various type relationships
    values[Anum_pg_type_typrelid - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_type_typsubscript - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_type_typelem - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_type_typarray - 1] = ObjectIdGetDatum(InvalidOid);

    // Use special shell I/O functions
    values[Anum_pg_type_typinput - 1] = ObjectIdGetDatum(F_SHELL_IN);
    values[Anum_pg_type_typoutput - 1] = ObjectIdGetDatum(F_SHELL_OUT);

    // Set remaining function OIDs to invalid
    values[Anum_pg_type_typreceive - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_type_typsend - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_type_typmodin - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_type_typmodout - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_type_typanalyze - 1] = ObjectIdGetDatum(InvalidOid);

    // Set storage and alignment characteristics
    values[Anum_pg_type_typalign - 1] = CharGetDatum(TYPALIGN_INT);
    values[Anum_pg_type_typstorage - 1] = CharGetDatum(TYPSTORAGE_PLAIN);
    values[Anum_pg_type_typnotnull - 1] = BoolGetDatum(false);
    values[Anum_pg_type_typbasetype - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_type_typtypmod - 1] = Int32GetDatum(-1);
    values[Anum_pg_type_typndims - 1] = Int32GetDatum(0);
    values[Anum_pg_type_typcollation - 1] = ObjectIdGetDatum(InvalidOid);

    // Set nullable fields
    nulls[Anum_pg_type_typdefaultbin - 1] = true;
    nulls[Anum_pg_type_typdefault - 1] = true;
    nulls[Anum_pg_type_typacl - 1] = true;

    // Get OID for the new type (handle binary upgrade case)
    if (IsBinaryUpgrade) {
        if (!OidIsValid(binary_upgrade_next_pg_type_oid))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("pg_type OID value not set when in binary upgrade mode")));
        typoid = binary_upgrade_next_pg_type_oid;
        binary_upgrade_next_pg_type_oid = InvalidOid;
    } else {
        typoid = GetNewOidWithIndex(pg_type_desc, TypeOidIndexId, Anum_pg_type_oid);
    }
    values[Anum_pg_type_oid - 1] = ObjectIdGetDatum(typoid);

    // Create and insert the tuple
    tup = heap_form_tuple(tupDesc, values, nulls);
    CatalogTupleInsert(pg_type_desc, tup);

    // Create dependencies (skip in bootstrap mode)
    if (!IsBootstrapProcessingMode()) {
        GenerateTypeDependencies(tup, pg_type_desc, NULL, NULL, 0,
                               false, false, true, false);
    }

    // Invoke post-creation hook and set return address
    InvokeObjectPostCreateHook(TypeRelationId, typoid, 0);
    ObjectAddressSet(address, TypeRelationId, typoid);

    // Cleanup and return
    heap_freetuple(tup);
    table_close(pg_type_desc, RowExclusiveLock);

    return address;
}
```