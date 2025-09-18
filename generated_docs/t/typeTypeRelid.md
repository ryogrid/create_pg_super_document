# typeTypeRelid

## Location
src/backend/parser/parse_type.c: 630 - 639

## Overview
Returns the relation OID (typrelid) associated with a PostgreSQL composite data type.

## Definition


## Detailed Description
The  function extracts the  attribute from a PostgreSQL type structure. The  field contains the OID of the relation (table) that corresponds to this type. This is primarily used for composite types, where the type is defined by a table structure, and row types, where each table automatically gets a corresponding row type.

For most built-in scalar types (like int4, text, etc.), this value is typically InvalidOid (0) since they don't correspond to any relation. However, for composite types created with CREATE TYPE or for row types that correspond to tables, this field contains the OID of the associated relation in the pg_class catalog.

## Parameters / Member Variables
- : A Type structure (HeapTuple) representing a row from the pg_type system catalog

## Dependencies
- Functions called/Symbols referenced:
  - Type (typedef for HeapTuple)
  - Form_pg_type (structure representing pg_type catalog row)
  - GETSTRUCT (macro to extract structure from HeapTuple)
  - Oid (object identifier type)
- Called from (representative examples):
  - FuncNameAsType (in parse_func.c:1896)

## Notes and Other Information
- This function is essential for handling composite types and row types in PostgreSQL
- The typrelid links the type system to the relation system, enabling composite types to be based on table definitions
- For scalar types, this typically returns InvalidOid (0)
- This function is part of the parser subsystem's type handling utilities
- The typrelid is used to look up the structure of composite types in the pg_class and pg_attribute catalogs