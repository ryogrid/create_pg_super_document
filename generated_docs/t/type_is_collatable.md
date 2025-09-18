# type_is_collatable

## Location
src/backend/utils/cache/lsyscache.c: 3081 - 3096

## Overview
Determines whether a given data type cares about collations, which is essential for operations that need to know if collation-sensitive comparison or sorting can be applied to values of that type.

## Definition
```c
bool type_is_collatable(Oid typid)
```

## Detailed Description
This function checks if a PostgreSQL data type supports collation by examining whether the type has a valid collation OID. Collation determines how text comparison and sorting should be performed for string-like data types. The function serves as a quick way to determine if collation-related operations are meaningful for a particular type. It works by calling `get_typcollation()` and checking if the returned OID is valid - if so, the type is collatable.

## Parameters / Member Variables
- `typid`: The OID of the PostgreSQL data type to check for collation support

## Dependencies
- Functions called/Symbols referenced:
  - get_typcollation
  - OidIsValid
- Called from (representative examples):
  - CheckAttributeType
  - coerce_to_target_type
  - transformCollateClause
  - ComputeIndexAttrs
  - DefineRange

## Notes and Other Information
- This function is crucial for query planning and type coercion decisions
- Text types like `text`, `varchar`, and `char` are typically collatable
- Numeric types, boolean, and other non-text types generally are not collatable
- Used extensively in parser and catalog operations to validate collation clauses
- The function is defined in `src/backend/utils/cache/lsyscache.c:3081-3096`