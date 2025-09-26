# JsonExpr

## Location
[src/include/nodes/primnodes.h:1813-1860](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L1813-L1860)

## Overview
JsonExpr represents the transformed representation of JSON_VALUE(), JSON_QUERY(), and JSON_EXISTS() functions in PostgreSQL's execution tree.

## Definition

```c
typedef struct JsonExpr
{
	Expr		xpr;

	JsonExprOp	op;

	char	   *column_name;	/* JSON_TABLE() column name or NULL if this is
								 * not for a JSON_TABLE() */

	/* jsonb-valued expression to query */
	Node	   *formatted_expr;

	/* Format of the above expression needed by ruleutils.c */
	JsonFormat *format;

	/* jsonpath-valued expression containing the query pattern */
	Node	   *path_spec;

	/* Expected type/format of the output. */
	JsonReturning *returning;

	/* Information about the PASSING argument expressions */
	List	   *passing_names;
	List	   *passing_values;

	/* User-specified or default ON EMPTY and ON ERROR behaviors */
	JsonBehavior *on_empty;
	JsonBehavior *on_error;

	/*
	 * Information about converting the result of jsonpath functions
	 * JsonPathQuery() and JsonPathValue() to the RETURNING type.
	 */
	bool		use_io_coercion;
	bool		use_json_coercion;

	/* WRAPPER specification for JSON_QUERY */
	JsonWrapper wrapper;

	/* KEEP or OMIT QUOTES for singleton scalars returned by JSON_QUERY() */
	bool		omit_quotes;

	/* JsonExpr's collation. */
	Oid			collation;

	/* Original JsonFuncExpr's location */
	ParseLoc	location;
} JsonExpr;
```
## Detailed Description
JsonExpr is a node type that represents JSON path expressions after parsing and transformation. It encapsulates all the necessary information for executing JSON_VALUE(), JSON_QUERY(), and JSON_EXISTS() operations, including the source JSON data, path specification, output format requirements, error handling behaviors, and various execution parameters.

## Parameters / Member Variables
- `xpr`: Base Expr structure for expression tree integration
- `op`: JsonExprOp specifying the type of JSON operation (VALUE, QUERY, EXISTS)
- `*column_name`: Name of JSON_TABLE() column, or NULL for standalone expressions
- `*formatted_expr`: The jsonb-valued expression to be queried
- `*format`: JsonFormat specification for the input expression format
- `*path_spec`: The jsonpath-valued expression containing the query pattern
- `*returning`: JsonReturning specification for expected output type and format
- `*passing_names`: List of parameter names for PASSING clause
- `*passing_values`: List of parameter values for PASSING clause
- `*on_empty`: JsonBehavior defining action when no results are found
- `*on_error`: JsonBehavior defining action when errors occur
- `use_io_coercion`: Flag indicating whether I/O coercion should be used for type conversion
- `use_json_coercion`: Flag indicating whether JSON-specific coercion should be used
- `wrapper`: JsonWrapper specification for JSON_QUERY result wrapping
- `omit_quotes`: Flag to keep or omit quotes for singleton scalars in JSON_QUERY()
- `collation`: Collation OID for string operations
- `location`: Parse location for error reporting
## Dependencies
- Functions called/Symbols referenced:
  - JsonExprOp
  - [JsonFormat](JsonFormat.md)
  - [JsonReturning](JsonReturning.md)
  - [JsonBehavior](JsonBehavior.md)
  - JsonWrapper
  - ParseLoc
- Called from (representative examples):
  - [ExecInitExprRec](../E/ExecInitExprRec.md)
  - [ExecInitJsonExpr](../E/ExecInitJsonExpr.md)
  - [transformJsonFuncExpr](../t/transformJsonFuncExpr.md)
  - [transformJsonTable](../t/transformJsonTable.md)

## Notes and Other Information
- Central to PostgreSQL's SQL/JSON functionality implementation
- Supports all major JSON path operations defined in SQL standard
- Integrates with PostgreSQL's expression evaluation framework
- Handles complex error and empty result behaviors as specified in SQL/JSON standard
- Used both in standalone JSON expressions and as part of JSON_TABLE operations