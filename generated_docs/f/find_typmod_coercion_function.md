# find_typmod_coercion_function

## Location
[src/backend/parser/parse_coerce.c:3318-3367](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L3318-L3367)

## Overview
Determines whether a given data type requires length coercion (type modifier adjustment) by searching for self-referential cast functions.

## Definition


## Detailed Description
find_typmod_coercion_function identifies types that need length coercion by looking for pg_cast entries where both source and target types are the same. This indicates that the type has a function to adjust its length or precision modifiers (typmod).

The function handles two main scenarios:
1. **Scalar types**: Direct lookup for self-referential casts (e.g., char(N) to char(M), numeric(p,s) to numeric(p',s'))
2. **Array types**: For true array types, the function examines the element type instead, as length coercion applies to array elements rather than the array structure itself

Common examples of types requiring length coercion include:
-  (char(N)) for length adjustment
-  for precision/scale adjustment  
-  for length constraint enforcement
-  and  for bit length adjustment

## Parameters / Member Variables
- : The OID of the data type to check for length coercion requirements
- : Pointer to store the OID of the length coercion function (set to InvalidOid if none found)

## Dependencies
- Functions called/Symbols referenced:
  - [typeidType](../t/typeidType.md)
  - IsTrueArrayType
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - Form_pg_type
  - Form_pg_cast
  - COERCION_PATH_FUNC
  - COERCION_PATH_ARRAYCOERCE  
  - COERCION_PATH_NONE
- Called from (representative examples):
  - [coerce_type_typmod](../c/coerce_type_typmod.md) (src/backend/parser/parse_coerce.c:777)

## Notes and Other Information
- The function specifically searches for casts where source and target types are identical
- For array types, attention shifts to the element type since ArrayCoerceExpr handles the array structure
- The returned funcid for array cases gets looked up again during ArrayCoerceExpr construction
- Only three result codes are possible: NONE (no coercion needed), FUNC (apply function), or ARRAYCOERCE (use ArrayCoerceExpr)
- This is a specialized subset of the more general find_coercion_pathway functionality
- Length coercion is essential for enforcing constraints like maximum string lengths or numeric precision
- The function assumes that any self-referential cast indicates length coercion capability