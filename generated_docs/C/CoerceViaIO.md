# CoerceViaIO

## Location
src/include/nodes/primnodes.h: 1204 - 1215

## Overview
CoerceViaIO is a PostgreSQL expression node that performs type coercion through input/output functions, converting values by serializing them to text and then parsing them back as the target type.

## Definition

```c
typedef struct CoerceViaIO
{
	Expr		xpr;
	Expr	   *arg;			/* input expression */
	Oid			resulttype;		/* output type of coercion */
	/* output typmod is not stored, but is presumed -1 */
	/* OID of collation, or InvalidOid if none */
	Oid			resultcollid pg_node_attr(query_jumble_ignore);
	/* how to display this node */
	CoercionForm coerceformat pg_node_attr(query_jumble_ignore);
	ParseLoc	location;		/* token location, or -1 if unknown */
} CoerceViaIO;
```
## Detailed Description
CoerceViaIO implements type coercion by using the output function of the source type to convert the value to its text representation, then using the input function of the target type to parse that text back into the desired type. This mechanism allows conversion between types that don't have direct cast functions but can be converted through their text representations.

This coercion path is used when PostgreSQL's type system determines that COERCION_PATH_COERCEVIAIO is the appropriate conversion method. It's particularly useful for converting between types where a direct cast function doesn't exist but both types have well-defined text input/output representations.

During execution, the process involves:
1. Calling the source type's output function to convert the value to text
2. Calling the target type's input function to parse the text into the target type
3. Handling any errors that occur during this conversion process

## Parameters / Member Variables
- : Base expression node structure
- : The input expression to be coerced
- : OID of the target type for the coercion
- : OID of the result collation, or InvalidOid if none (ignored for query jumbling)
- : Controls how this coercion is displayed (COERCE_EXPLICIT_CAST, COERCE_IMPLICIT_CAST, etc.)
- : Parse location in the original query, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - Type-specific output functions (determined at runtime)
  - Type-specific input functions (determined at runtime)
  - makeNode (for creating CoerceViaIO nodes)
  - Various executor evaluation functions
- Called from (representative examples):
  - coerce_to_target_type (in parse_coerce.c)
  - ExecInitExprRec (in execExpr.c)
  - eval_const_expressions_mutator (in clauses.c)
  - plpgsql type coercion fallback (in pl_exec.c)

## Notes and Other Information
- The output typmod is always presumed to be -1 (no specific type modifier)
- CoerceViaIO can be a performance bottleneck since it involves string conversion
- The executor has both regular (EEOP_IOCOERCE) and safe (EEOP_IOCOERCE_SAFE) evaluation paths
- Used as a fallback mechanism when no direct cast function exists between types
- Commonly seen in assignment casts to string types and explicit casts from string types
- The coercion can fail at runtime if the text representation cannot be parsed as the target type