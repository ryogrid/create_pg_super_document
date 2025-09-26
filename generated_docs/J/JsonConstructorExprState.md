# JsonConstructorExprState

## Location
[src/include/executor/execExpr.h:768-780](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/execExpr.h#L768-L780)

## Overview
JsonConstructorExprState manages the execution state for JSON constructor expressions, providing workspace for argument evaluation and type conversion caching during JSON object and array construction.

## Definition

```c
typedef struct JsonConstructorExprState
{
	JsonConstructorExpr *constructor;
	Datum	   *arg_values;
	bool	   *arg_nulls;
	Oid		   *arg_types;
	struct
	{
		int			category;
		Oid			outfuncid;
	}		   *arg_type_cache; /* cache for datum_to_json[b]() */
	int			nargs;
} JsonConstructorExprState;
```
## Detailed Description
JsonConstructorExprState provides the execution state and workspace needed for evaluating JSON constructor expressions (JSON_OBJECT, JSON_ARRAY, etc.). The structure is designed as out-of-line state because it's too large to fit efficiently within the inline data union of ExprEvalStep.

The state manages arrays of argument values, null flags, and type information, along with a specialized cache for type conversion operations. The arg_type_cache optimizes repeated conversions from PostgreSQL data types to JSON format by caching the type category and output function ID for each argument type.

This structure works closely with the expression evaluator to efficiently construct JSON objects and arrays from SQL expressions, handling type coercion and null value processing as needed.

## Parameters / Member Variables
- `constructor`: Pointer to the original JsonConstructorExpr parse node containing the expression definition
- `arg_values`: Array storing the evaluated Datum values for each argument to the JSON constructor
- `arg_nulls`: Array storing null flags corresponding to each argument value
- `arg_types`: Array storing the OID of the data type for each argument
- `arg_type_cache`: Array of cache structures for optimizing type conversions, each containing:
  - `category`: Type category for the argument (used by datum_to_json functions)
  - `outfuncid`: OID of the output function for converting this type to JSON format
- `nargs`: Total number of arguments to the JSON constructor expression

## Dependencies
- Functions called/Symbols referenced:
  - [JsonConstructorExpr](JsonConstructorExpr.md) (the parse node structure for JSON constructor expressions)
- Called from (representative examples):
  - [ExecInitExprRec](../E/ExecInitExprRec.md) (expression initialization and state setup)
  - [ExecEvalJsonConstructor](../E/ExecEvalJsonConstructor.md) (expression evaluation)
  - [ExprEvalStep](../E/ExprEvalStep.md) (referenced in json_constructor union member)

## Notes and Other Information
- Designed as out-of-line state due to size constraints in ExprEvalStep
- The type cache significantly improves performance for repeated evaluations with the same argument types
- Supports various JSON constructor expressions including JSON_OBJECT and JSON_ARRAY
- Handles both JSONB and JSON output formats through the datum_to_json/datum_to_jsonb functions
- Used in EEOP_JSON_CONSTRUCTOR expression evaluation steps
- The cache structure avoids repeated lookups of type conversion functions during expression evaluation
- Manages memory for argument arrays that are allocated based on the number of constructor arguments