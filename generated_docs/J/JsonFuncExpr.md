# JsonFuncExpr

## Location
[src/include/nodes/parsenodes.h:1785-1800](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L1785-L1800)

## Overview
A structure representing untransformed function expressions for SQL/JSON query functions, providing comprehensive support for JSON path-based operations with various behavioral options.

## Definition

```c
typedef struct JsonFuncExpr
{
	NodeTag		type;
	JsonExprOp	op;				/* expression type */
	char	   *column_name;	/* JSON_TABLE() column name or NULL if this is
								 * not for a JSON_TABLE() */
	JsonValueExpr *context_item;	/* context item expression */
	Node	   *pathspec;		/* JSON path specification expression */
	List	   *passing;		/* list of PASSING clause arguments, if any */
	JsonOutput *output;			/* output clause, if specified */
	JsonBehavior *on_empty;		/* ON EMPTY behavior */
	JsonBehavior *on_error;		/* ON ERROR behavior */
	JsonWrapper wrapper;		/* array wrapper behavior (JSON_QUERY only) */
	JsonQuotes	quotes;			/* omit or keep quotes? (JSON_QUERY only) */
	ParseLoc	location;		/* token location, or -1 if unknown */
} JsonFuncExpr;
```
## Detailed Description
JsonFuncExpr is a comprehensive parse tree node representing SQL/JSON query functions in their untransformed state. It encapsulates all aspects of JSON query operations including the operation type, context item, JSON path specification, parameter passing, output formatting, and error/empty handling behaviors. This structure serves as the foundation for JSON functions like JSON_VALUE, JSON_QUERY, and JSON_EXISTS, providing a unified representation before transformation into executable forms. The structure supports both standalone JSON functions and JSON_TABLE() column specifications, making it versatile across different JSON operation contexts.

## Parameters / Member Variables
- : Standard NodeTag for PostgreSQL node identification
- : JsonExprOp enum specifying the type of JSON expression (VALUE, QUERY, EXISTS, etc.)
- : Name of the column when used in JSON_TABLE context (NULL otherwise)
- : Pointer to JsonValueExpr providing the JSON context for the operation
- : Pointer to Node containing the JSON path specification expression
- : List of JsonArgument nodes from PASSING clause for parameter binding
- : Pointer to JsonOutput specifying return type and format options
- : Pointer to JsonBehavior defining behavior when no data is found
- : Pointer to JsonBehavior defining behavior when errors occur
- : JsonWrapper enum controlling array wrapping behavior (JSON_QUERY specific)
- : JsonQuotes enum controlling quote handling (JSON_QUERY specific)
- : ParseLoc indicating source code position for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - JsonExprOp
  - [JsonValueExpr](JsonValueExpr.md)
  - [JsonOutput](JsonOutput.md)
  - [JsonBehavior](JsonBehavior.md)
  - JsonWrapper
  - JsonQuotes
  - ParseLoc
- Called from (representative examples):
  - [raw_expression_tree_walker_impl](../r/raw_expression_tree_walker_impl.md)
  - [transformExprRecurse](../t/transformExprRecurse.md)
  - [transformJsonFuncExpr](../t/transformJsonFuncExpr.md)
  - [transformJsonTable](../t/transformJsonTable.md)
  - [transformJsonTableColumns](../t/transformJsonTableColumns.md)
  - [transformJsonTableColumn](../t/transformJsonTableColumn.md)
  - [FigureColnameInternal](../F/FigureColnameInternal.md)

## Notes and Other Information
- Central structure for PostgreSQL's SQL/JSON query function support
- Supports multiple JSON operation types through the op field (JSON_VALUE, JSON_QUERY, JSON_EXISTS, etc.)
- Provides comprehensive error and empty result handling through dedicated behavior specifications
- The wrapper and quotes fields are specific to JSON_QUERY operations and control output formatting
- Can represent both standalone JSON functions and columns within JSON_TABLE expressions
- Used extensively throughout JSON parsing and transformation pipeline
- Location tracking enables precise error reporting during parsing and transformation phases
- Located in src/include/nodes/parsenodes.h at lines 1785-1800