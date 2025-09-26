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