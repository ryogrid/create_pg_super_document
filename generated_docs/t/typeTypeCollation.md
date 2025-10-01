# typeTypeCollation

## Location
[src/backend/parser/parse_type.c:640-653](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_type.c#L640-L653)

## Overview
Returns the default collation OID (typcollation) associated with a PostgreSQL data type.

## Definition

```c
Oid
typeTypeCollation(Type typ)
```
## Detailed Description
The  function extracts the  attribute from a PostgreSQL type structure. The  field contains the OID of the default collation for this data type. Collations define the rules for comparing and sorting text data, including locale-specific behaviors for character ordering, case sensitivity, and accent sensitivity.

This attribute is primarily relevant for character-based types (like text, varchar, char) and determines how string comparisons and sorting operations behave by default. For types that don't support collations (like integers, dates, etc.), this field typically contains InvalidOid (0).

The default collation can be overridden at the column level or in specific operations, but this function returns the type's inherent default collation.

## Parameters / Member Variables
- : A Type structure (HeapTuple) representing a row from the pg_type system catalog

## Dependencies
- Functions called/Symbols referenced:
  - Type (typedef for HeapTuple)
  - Form_pg_type (structure representing pg_type catalog row)
  - GETSTRUCT (macro to extract structure from HeapTuple)
  - Oid (object identifier type)
- Called from (representative examples):
  - [coerce_type](../c/coerce_type.md) (in parse_coerce.c:288)

## Notes and Other Information
- This function is crucial for PostgreSQL's internationalization and text handling capabilities
- The collation system was introduced in PostgreSQL 9.1 to support locale-aware text operations
- For non-collatable types, this typically returns InvalidOid (0)
- The returned OID can be used to look up collation details in the pg_collation catalog
- This function is part of the parser subsystem's type handling utilities
- Collations affect the behavior of comparison operators, ORDER BY clauses, and text-processing functions

## Simplified Source

```c
Oid typeTypeCollation(Type typ) {
    // Extract the type structure from the heap tuple
    Form_pg_type typtup = (Form_pg_type) GETSTRUCT(typ);

    // Return the default collation OID for this type
    // Non-collatable types return InvalidOid (0)
    return typtup->typcollation;
}
```

**Simplification Notes:**
- Added explanatory comments about collation purpose
- Function is already minimal, so only added documentation
- Core logic: extract type structure and return the collation OID
- Preserved the essential purpose: provide default collation information for the type