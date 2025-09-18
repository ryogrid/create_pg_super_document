# TypeCreate

## Location
[src/backend/catalog/pg_type.c:195-556](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_type.c#L195-L556)

## Overview
TypeCreate is the comprehensive function that creates a fully-defined type entry in the pg_type system catalog, handling both new type creation and updating existing shell types with complete type definitions.

## Definition


## Detailed Description
TypeCreate is the core function responsible for creating complete type definitions in PostgreSQL's type system. It performs extensive validation of type parameters, ensures consistency between size, alignment, and pass-by-value semantics, and handles both new type creation and shell type completion.

The function validates that internal size specifications are appropriate (positive for fixed-length, -1 for varlena, -2 for cstring), checks alignment constraints match the type's characteristics, and ensures storage options are compatible with the type's structure. For pass-by-value types, it enforces strict rules about supported sizes (char, int16, int32, or Datum on 64-bit systems) and their corresponding alignments.

The function can operate in two modes: creating a completely new type or updating an existing shell type. When updating a shell type (created by TypeShellMake), it verifies ownership consistency and replaces placeholder values with actual type specifications. It handles dependent types (implicit arrays, multirange types, relation rowtypes) differently by not creating ACL entries and managing dependencies through the related objects.

## Parameters
- : Specific OID to use for the type (0 for auto-assignment)
- : Name of the type being created
- : OID of the schema containing the type
- : OID of related relation (for composite types)
- : Kind of related relation (for composite types)
- : OID of the type owner
- : Internal storage size (-2=cstring, -1=varlena, >0=fixed)
- : Type category (base, composite, domain, enum, etc.)
- : General category for type (numeric, string, etc.)
- : Whether this is the preferred type in its category
- : Array element delimiter character
- : OID of input function (text to internal)
- : OID of output function (internal to text)
- : OID of binary input function
- : OID of binary output function
- : OID of type modifier input function
- : OID of type modifier output function
- : OID of analyze function for statistics
- : OID of subscripting function
- : OID of array element type (for array types)
- : Whether this is an implicitly created array type
- : OID of corresponding array type
- : OID of base type (for domains)
- : Human-readable default value
- : Binary representation of default value
- : Whether values are passed by value or reference
- : Storage alignment requirement (char, short, int, double)
- : TOAST storage strategy (plain, external, extended, main)
- : Type-specific modifier value
- : Number of array dimensions (for domains over arrays)
- : Whether type has NOT NULL constraint
- : OID of collation for the type

## Dependencies
- Functions called/Symbols referenced:
  - namestrcpy, NameGetDatum, Int16GetDatum, CharGetDatum, BoolGetDatum, ObjectIdGetDatum, Int32GetDatum
  - CStringGetTextDatum, PointerGetDatum
  - [get_user_default_acl](../g/get_user_default_acl.md)
  - table_open, table_close
  - SearchSysCacheCopy2
  - [heap_modify_tuple](../h/heap_modify_tuple.md), heap_form_tuple
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md), CatalogTupleInsert
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - IsBootstrapProcessingMode
  - [GenerateTypeDependencies](../G/GenerateTypeDependencies.md)
  - [stringToNode](../s/stringToNode.md)
  - InvokeObjectPostCreateHook
  - ObjectAddressSet
  - [aclcheck_error](../a/aclcheck_error.md)
- Called from (representative examples):
  - [AddNewRelationType](../A/AddNewRelationType.md) (src/backend/catalog/heap.c:1036)
  - [heap_create_with_catalog](../h/heap_create_with_catalog.md) (src/backend/catalog/heap.c:1359)
  - [DefineType](../D/DefineType.md) (src/backend/commands/typecmds.c:573, 615)
  - [DefineDomain](../D/DefineDomain.md) (src/backend/commands/typecmds.c:1024, 1065)
  - [DefineEnum](../D/DefineEnum.md) (src/backend/commands/typecmds.c:1187, 1228)
  - [DefineRange](../D/DefineRange.md) (src/backend/commands/typecmds.c:1529, 1596, 1639, 1678)

## Notes and Other Information
- Performs comprehensive validation of type size, alignment, and pass-by-value consistency
- Handles both new type creation and shell type completion in a single interface
- Supports binary upgrade mode with predetermined OIDs
- Manages dependent type relationships (arrays, multirange, composite types) with special dependency handling
- Only varlena types (internal size -1) can use non-PLAIN storage strategies for TOAST
- Pass-by-value types are restricted to specific sizes with matching alignment requirements
- Creates appropriate ACL entries except for dependent types which inherit permissions
- The function sets  to true, marking the type as fully defined and usable