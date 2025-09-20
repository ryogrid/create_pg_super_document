# TypeCategory

## Location
[src/backend/parser/parse_coerce.c:2978-2996](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L2978-L2996)

## Overview
Assigns a type category to the specified type OID, used for type resolution and coercion decisions.

## Definition

```c
TYPCATEGORY
TypeCategory(Oid type)
```
## Detailed Description
This function retrieves the type category for a given type OID. Type categories are used throughout PostgreSQL's type system for making decisions about implicit type coercion, operator resolution, and function overloading. Each data type in PostgreSQL belongs to exactly one category, which groups related types together for coercion purposes.

The function is a simple wrapper around  that extracts only the category information (ignoring the preferred type flag) and ensures that a valid category is always returned.

Type categories help PostgreSQL determine which types can be implicitly converted to which other types. For example, types within the same category may have more liberal coercion rules between them, while cross-category coercion may be more restricted or require explicit casting.

## Parameters / Member Variables
- : The OID of the type for which to retrieve the category

## Dependencies
- Functions called/Symbols referenced:
  - [get_type_category_preferred](../g/get_type_category_preferred.md)
  - TYPCATEGORY_INVALID
  - TYPCATEGORY (type definition)
- Called from (representative examples):
  - [GetDefaultOpClass](../G/GetDefaultOpClass.md)
  - [find_coercion_pathway](../f/find_coercion_pathway.md)
  - [func_select_candidate](../f/func_select_candidate.md)
  - [func_get_detail](../f/func_get_detail.md)
  - [transformJsonBehavior](../t/transformJsonBehavior.md)

## Notes and Other Information
- Located in src/backend/parser/parse_coerce.c:2978-2996
- Returns a TYPCATEGORY value (typedef for char)
- Must never return TYPCATEGORY_INVALID (enforced by Assert)
- Categories include:
  - TYPCATEGORY_ARRAY ('A') - Array types
  - TYPCATEGORY_BOOLEAN ('B') - Boolean type
  - TYPCATEGORY_COMPOSITE ('C') - Composite/record types
  - TYPCATEGORY_DATETIME ('D') - Date/time types
  - TYPCATEGORY_ENUM ('E') - Enumerated types
  - TYPCATEGORY_GEOMETRIC ('G') - Geometric types
  - TYPCATEGORY_NETWORK ('I') - Network address types
  - TYPCATEGORY_NUMERIC ('N') - Numeric types
  - TYPCATEGORY_PSEUDOTYPE ('P') - Pseudotypes
  - TYPCATEGORY_RANGE ('R') - Range types
  - TYPCATEGORY_STRING ('S') - String types
  - TYPCATEGORY_TIMESPAN ('T') - Time interval types
  - TYPCATEGORY_USER ('U') - User-defined types
  - TYPCATEGORY_BITSTRING ('V') - Bit string types
  - TYPCATEGORY_UNKNOWN ('X') - Unknown type
  - TYPCATEGORY_INTERNAL ('Z') - Internal types
- Critical for PostgreSQL's operator and function resolution algorithms