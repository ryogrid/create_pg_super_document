# is_complex_array

## Location
[src/backend/parser/parse_coerce.c:3368-3381](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L3368-L3381)

## Overview
Determines whether a given type is an array of composite (complex) types by checking if its element type is composite.

## Definition


## Detailed Description
is_complex_array is a utility function that identifies array types whose elements are composite types (structs, records, or user-defined types). This classification is important for type coercion decisions, particularly when determining if an array can be coerced to RECORD[] or when handling complex type conversions.

The function works by:
1. Extracting the element type OID from the given array type using get_element_type()
2. Checking if that element type is composite using the ISCOMPLEX() macro
3. Returning true only if both conditions are met (valid element type AND composite)

This function is used internally within the type coercion system to make decisions about allowable type conversions, especially in the context of binary coercibility checks.

## Parameters / Member Variables
- : The OID of the type to test for being a composite array

## Dependencies
- Functions called/Symbols referenced:
  - [get_element_type](../g/get_element_type.md)
  - ISCOMPLEX
- Called from (representative examples):
  - [coerce_type](../c/coerce_type.md) (src/backend/parser/parse_coerce.c:495, 502)
  - [can_coerce_type](../c/can_coerce_type.md) (src/backend/parser/parse_coerce.c:623, 631)  
  - [IsBinaryCoercibleWithCast](../I/IsBinaryCoercibleWithCast.md) (src/backend/parser/parse_coerce.c:3105)

## Notes and Other Information
- This is a static function, only visible within parse_coerce.c
- Does not return true for record[] arrays (RECORDARRAYOID) - callers must check for that separately if needed
- The function is a simple composition of get_element_type() and ISCOMPLEX() checks
- Used primarily in type coercion logic to determine valid conversion pathways for complex array types
- Composite types include user-defined types, table row types, and other structured data types
- The distinction between complex arrays and simple arrays is crucial for PostgreSQL's type coercion system