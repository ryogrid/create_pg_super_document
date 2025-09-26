# transformTypeCast

## Location
[src/backend/parser/parse_expr.c:2692-2775](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L2692-L2775)

## Overview
Handles explicit CAST constructs in PostgreSQL by transforming the argument, looking up the target type name, and applying necessary coercion functions to convert from one data type to another.

## Definition

```c
struct and the target
	 * type is an array type, we invoke transformArrayExpr() directly so that
	 * we can pass down the type information.  This avoids some cases where
	 * transformArrayExpr() might not infer the correct type.  Otherwise, just
	 * transform the argument normally.
	 */
	if (IsA(arg, A_ArrayExpr))
	{
		Oid			targetBaseType;
		int32		targetBaseTypmod;
		Oid			elementType;

		/*
		 * If target is a domain over array, work with the base array type
		 * here.  Below, we'll cast the array type to the domain.  In the
		 * usual case that the target is not a domain, the remaining steps
		 * will be a no-op.
		 */
		targetBaseTypmod = targetTypmod;
		targetBaseType = getBaseTypeAndTypmod(targetType, &targetBaseTypmod);
		elementType = get_element_type(targetBaseType);
		if (OidIsValid(elementType))
		{
			expr = transformArrayExpr(pstate,
									  (A_ArrayExpr *) arg,
									  targetBaseType,
									  elementType,
									  targetBaseTypmod);
		}
		else
			expr = transformExprRecurse(pstate, arg);
	}
	else
		expr = transformExprRecurse(pstate, arg);
```
## Detailed Description
The  function is responsible for processing explicit type cast operations in SQL expressions (e.g.,  or ). It performs type conversion by first determining the target type and then applying the appropriate coercion mechanisms. The function includes special handling for array expressions, where it can pass down type information to improve type inference. When the target type is an array and the source is an ARRAY[] construct, it invokes  directly to ensure correct type handling. For domain types over arrays, it works with the base array type first and then casts to the domain. The function validates that the conversion is possible and reports appropriate errors if the cast cannot be performed.

## Parameters
- `pstate`: ParseState pointer containing the current parsing context and state information
- `tc`: TypeCast pointer containing the cast expression with the source argument and target type information

## Dependencies
- Functions called/Symbols referenced:
  - [typenameTypeIdAndMod](typenameTypeIdAndMod.md)
  - [getBaseTypeAndTypmod](../g/getBaseTypeAndTypmod.md)  
  - [get_element_type](../g/get_element_type.md)
  - [transformArrayExpr](transformArrayExpr.md)
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