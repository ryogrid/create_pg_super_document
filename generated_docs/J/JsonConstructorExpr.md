# JsonConstructorExpr

## Location
[src/include/nodes/primnodes.h:1703-1714](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L1703-L1714)

## Overview
JsonConstructorExpr is a wrapper over FuncExpr/Aggref/WindowFunc for SQL/JSON constructor functions, providing a unified interface for JSON object and array construction operations.

## Definition
```c
typedef struct JsonConstructorExpr
{
	Expr		xpr;
	JsonConstructorType type;	/* constructor type */
	List	   *args;
	Expr	   *func;			/* underlying json[b]_xxx() function call */
	Expr	   *coercion;		/* coercion to RETURNING type */
	JsonReturning *returning;	/* RETURNING clause */
	bool		absent_on_null; /* ABSENT ON NULL? */
	bool		unique;			/* WITH UNIQUE KEYS? (JSON_OBJECT[AGG] only) */
	ParseLoc	location;
} JsonConstructorExpr;
```

## Detailed Description
JsonConstructorExpr serves as a unified wrapper around various PostgreSQL expression types (FuncExpr, Aggref, WindowFunc) specifically for SQL/JSON constructor functions. This abstraction allows the system to handle JSON object and array construction operations with consistent semantics while supporting various JSON-specific features like RETURNING clauses, null handling options, and uniqueness constraints.

The structure encapsulates both the underlying function call that performs the actual JSON construction and additional metadata about how the construction should behave. This includes type coercion for RETURNING clauses, null handling policies, and uniqueness requirements for JSON objects.

The design allows PostgreSQL to optimize and execute JSON constructor expressions efficiently while maintaining the rich semantics required by the SQL/JSON standard.

## Parameters / Member Variables
- `xpr`: Base Expr structure for standard expression handling
- `type`: JsonConstructorType enum specifying the specific constructor type (OBJECT, ARRAY, etc.)
- `args`: List of arguments to be passed to the JSON constructor function
- `func`: Pointer to the underlying json[b]_xxx() function call that performs the actual construction
- `coercion`: Expression for type coercion to match RETURNING clause requirements
- `returning`: Pointer to JsonReturning structure containing RETURNING clause specifications
- `absent_on_null`: Boolean flag indicating whether NULL values should be omitted (ABSENT ON NULL)
- `unique`: Boolean flag for unique key enforcement (WITH UNIQUE KEYS for JSON_OBJECT[AGG] only)
- `location`: Parse location for error reporting and debugging

## Dependencies
- Functions called/Symbols referenced:
  - JsonConstructorType
  - JsonReturning
  - ParseLoc
  - Expr
  - List

- Called from (representative examples):
  - ExecInitExprRec
  - ExecEvalJsonConstructor
  - makeJsonConstructorExpr
  - get_json_constructor
  - get_json_constructor_options
  - get_json_agg_constructor
  - contain_mutable_functions_walker
  - exprType
  - exprTypmod
  - exprCollation
  - exprSetCollation
  - exprLocation

## Notes and Other Information
- This structure unifies handling of different JSON constructor types under a single interface
- The absent_on_null flag controls behavior when encountering NULL values during construction
- The unique flag is specific to JSON object constructors and enforces key uniqueness
- The wrapper design allows for consistent optimization and execution across different underlying function types
- Located in src/include/nodes/primnodes.h:1703-1714
- Part of PostgreSQL's comprehensive SQL/JSON implementation supporting the SQL standard