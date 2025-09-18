# transformTypeCast

## Location
[src/backend/parser/parse_expr.c:2692-2775](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L2692-L2775)

## Overview
Handles explicit CAST constructs in PostgreSQL by transforming the argument, looking up the target type name, and applying necessary coercion functions to convert from one data type to another.

## Definition


## Detailed Description
The  function is responsible for processing explicit type cast operations in SQL expressions (e.g.,  or ). It performs type conversion by first determining the target type and then applying the appropriate coercion mechanisms. The function includes special handling for array expressions, where it can pass down type information to improve type inference. When the target type is an array and the source is an ARRAY[] construct, it invokes  directly to ensure correct type handling. For domain types over arrays, it works with the base array type first and then casts to the domain. The function validates that the conversion is possible and reports appropriate errors if the cast cannot be performed.

## Parameters / Member Variables
- : ParseState pointer containing the current parsing context and state information
- : TypeCast pointer containing the cast expression with the source argument and target type information

## Dependencies
- Functions called/Symbols referenced:
  - [typenameTypeIdAndMod](typenameTypeIdAndMod.md)
  - [getBaseTypeAndTypmod](../g/getBaseTypeAndTypmod.md)  
  - [get_element_type](../g/get_element_type.md)
  - transformArrayExpr
  - [transformExprRecurse](transformExprRecurse.md)
  - [coerce_to_target_type](../c/coerce_to_target_type.md)
  - [parser_coercion_errposition](../p/parser_coercion_errposition.md)
- Called from (representative examples):
  - [transformExprRecurse](transformExprRecurse.md)

## Notes and Other Information
- The function prioritizes the location of the :: or CAST symbol for error reporting, falling back to the type name location if unavailable
- Special optimization for ARRAY[] constructs when casting to array types to improve type inference
- Handles domain types over arrays by working with the base array type first
- Uses COERCION_EXPLICIT and COERCE_EXPLICIT_CAST flags to indicate this is an explicit user-requested cast
- Returns the original expression unchanged if the input type is InvalidOid (NULL input)
- Reports detailed error messages including source and target type names when casts are impossible