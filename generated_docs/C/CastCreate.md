# CastCreate

## Location
src/backend/catalog/pg_cast.c: 49 - 138

## Overview
Creates a new type cast in the PostgreSQL catalog by forming and inserting tuples into pg_cast, along with proper dependency tracking for all related objects.

## Definition


## Detailed Description
CastCreate is responsible for creating a new cast entry in the PostgreSQL system catalog. It performs several critical operations: validates that the cast doesn't already exist, assigns a new OID, creates the catalog tuple with all necessary attributes, and establishes dependency relationships between the cast and its dependent objects (source type, target type, cast function, and any required intermediate casts). The function handles both function-based and binary-compatible casts, ensuring proper dependency tracking for automatic cleanup when dependent objects are dropped.

## Parameters / Member Variables
- : OID of the source data type being cast from
- : OID of the target data type being cast to  
- : OID of the cast function (InvalidOid for binary coercible casts)
- : OID of input cast required for binary coercibility (InvalidOid if none)
- : OID of output cast required for binary coercibility (InvalidOid if none)
- : Context in which the cast can be invoked ('e' = explicit, 'a' = assignment, 'i' = implicit)
- : Method of casting ('f' = function, 'i' = inout, 'b' = binary compatible)
- : Dependency type for relationships with referenced objects

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache2
  - GetNewOidWithIndex  
  - heap_form_tuple
  - CatalogTupleInsert
  - ObjectAddressSet
  - add_exact_object_address
  - record_object_address_dependencies
  - recordDependencyOnCurrentExtension
  - InvokeObjectPostCreateHook
  - heap_freetuple
- Called from (representative examples):
  - CreateCast (src/backend/commands/functioncmds.c:1777)
  - DefineRange (src/backend/commands/typecmds.c:1718)

## Notes and Other Information
The function performs duplicate checking before insertion using SearchSysCache2 to provide user-friendly error messages. It creates dependencies not only on the primary objects (source/target types, cast function) but also on any intermediate casts that may be required for binary coercibility. Extension dependencies are automatically recorded, and post-creation hooks are invoked for proper system integration. Memory cleanup is handled through heap_freetuple and proper relation closing.