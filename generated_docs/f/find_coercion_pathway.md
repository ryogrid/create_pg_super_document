# find_coercion_pathway

## Location
[src/backend/parser/parse_coerce.c:3155-3317](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L3155-L3317)

## Overview
Searches for a coercion pathway between two scalar data types, determining the method and function needed for type conversion based on the specified coercion context.

## Definition

```c
enum */
		switch (castForm->castcontext)
		{
			case COERCION_CODE_IMPLICIT:
				castcontext = COERCION_IMPLICIT;
				break;
			case COERCION_CODE_ASSIGNMENT:
				castcontext = COERCION_ASSIGNMENT;
				break;
			case COERCION_CODE_EXPLICIT:
				castcontext = COERCION_EXPLICIT;
				break;
			default:
				elog(ERROR, "unrecognized castcontext: %d",
					 (int) castForm->castcontext);
				castcontext = 0;	/* keep compiler quiet */
				break;
		}

		/* Rely on ordering of enum for correct behavior here */
		if (ccontext >= castcontext)
		{
			switch (castForm->castmethod)
			{
				case COERCION_METHOD_FUNCTION:
					result = COERCION_PATH_FUNC;
					*funcid = castForm->castfunc;
					break;
				case COERCION_METHOD_INOUT:
					result = COERCION_PATH_COERCEVIAIO;
					break;
				case COERCION_METHOD_BINARY:
					result = COERCION_PATH_RELABELTYPE;
					break;
				default:
					elog(ERROR, "unrecognized castmethod: %d",
						 (int) castForm->castmethod);
					break;
			}
		}

		ReleaseSysCache(tuple);
```
## Detailed Description
find_coercion_pathway is the core function for determining how to convert between PostgreSQL data types. It implements a comprehensive search strategy that considers multiple coercion mechanisms and respects coercion context restrictions.

The function follows a hierarchical search strategy:
1. **Domain resolution**: Reduces domain types to their base types for comparison
2. **pg_cast lookup**: Searches the system catalog for explicit cast definitions
3. **Array coercion**: Attempts element-wise array conversion using recursive calls
4. **I/O coercion**: Falls back to string-based conversion through input/output functions
5. **PL/pgSQL special case**: Allows I/O coercion for PL/pgSQL assignments when no other path exists

The function returns one of several path types indicating the coercion method required, along with a function OID when applicable.

## Parameters / Member Variables
- : The OID of the target data type to convert to
- : The OID of the source data type to convert from
- : The coercion context (implicit, assignment, explicit, or PL/pgSQL) that determines available casts
- : Pointer to store the OID of the coercion function (set to InvalidOid for non-function coercions)

## Dependencies
- Functions called/Symbols referenced:
  - [getBaseType](../g/getBaseType.md)
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - [get_element_type](../g/get_element_type.md)
  - [find_coercion_pathway](find_coercion_pathway.md) (recursive call)
  - [TypeCategory](../T/TypeCategory.md)
  - Form_pg_cast
  - CoercionContext enums (COERCION_IMPLICIT, COERCION_ASSIGNMENT, COERCION_EXPLICIT, COERCION_PLPGSQL)
  - [CoercionPathType](../C/CoercionPathType.md) enums (COERCION_PATH_NONE, COERCION_PATH_FUNC, COERCION_PATH_RELABELTYPE, etc.)
- Called from (representative examples):
  - [coerce_type](../c/coerce_type.md) (src/backend/parser/parse_coerce.c:413)
  - [can_coerce_type](../c/can_coerce_type.md) (src/backend/parser/parse_coerce.c:596)
  - [func_get_detail](func_get_detail.md) (src/backend/parser/parse_func.c:1504)
  - [ri_HashCompareOp](../r/ri_HashCompareOp.md) (src/backend/utils/adt/ri_triggers.c:2966)

## Notes and Other Information
- Currently handles only scalar types, not polymorphic types or composite type casts
- Domain types are automatically reduced to base types before processing
- COERCION_PATH_RELABELTYPE doesn't guarantee zero-effort conversion due to potential domain constraints
- Array coercion is disabled for oidvector and int2vector to prevent inappropriate captures
- I/O coercion provides fallback compatibility, especially for string type conversions
- The function uses enum ordering to efficiently check coercion context compatibility
- Recursive calls enable element-wise array coercion when direct array casts aren't available
- PL/pgSQL context gets special treatment allowing I/O coercion as last resort