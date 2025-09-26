# JsonScalarExpr

## Location
[src/include/nodes/parsenodes.h:1896-1902](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L1896-L1902)

## Overview
JsonScalarExpr represents the untransformed representation of the JSON_SCALAR() function call in PostgreSQL's SQL/JSON implementation, used to convert scalar values to JSON format.

## Definition

```c
typedef struct JsonScalarExpr
{
	NodeTag		type;
	Expr	   *expr;			/* scalar expression */
	JsonOutput *output;			/* RETURNING clause, if specified */
	ParseLoc	location;		/* token location, or -1 if unknown */
} JsonScalarExpr;
```
## Detailed Description
JsonScalarExpr is a parse tree node that represents a JSON_SCALAR() function call before transformation. This structure is part of PostgreSQL's SQL/JSON standard implementation, specifically handling the conversion of scalar values to JSON format. The structure maintains the original scalar expression along with optional output formatting specifications and location information for error reporting.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a JsonScalarExpr node
- `*expr`: Pointer to the scalar expression that will be converted to JSON
- `*output`: Optional JsonOutput structure specifying RETURNING clause details for format control
- `location`: Parse location information for error reporting and debugging (-1 if unknown)
## Dependencies
- Functions called/Symbols referenced:
  - [JsonOutput](JsonOutput.md)
  - ParseLoc
- Called from (representative examples):
  - [raw_expression_tree_walker_impl](../r/raw_expression_tree_walker_impl.md)
  - [transformExprRecurse](../t/transformExprRecurse.md)
  - [transformJsonScalarExpr](../t/transformJsonScalarExpr.md)

## Notes and Other Information
- This is part of PostgreSQL's implementation of the SQL/JSON standard
- The structure is used during the parsing phase before transformation to executable form
- The location field aids in providing accurate error messages during query compilation
- The output field allows for flexible JSON formatting options through the RETURNING clause