# JsonValueExpr

## Location
[src/include/nodes/primnodes.h:1680-1686](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L1680-L1686)

## Overview
JsonValueExpr represents a JSON value expression with optional FORMAT clause, containing both the original user-specified expression and a formatted version for execution.

## Definition
```c
typedef struct JsonValueExpr
{
	NodeTag		type;
	Expr	   *raw_expr;		/* user-specified expression */
	Expr	   *formatted_expr; /* coerced formatted expression */
	JsonFormat *format;			/* FORMAT clause, if specified */
} JsonValueExpr;
```

## Detailed Description
JsonValueExpr serves as the internal representation of JSON value expressions that can include a FORMAT clause (expr [FORMAT JsonFormat]). This structure maintains both the original user-specified expression and a coerced formatted version.

The dual expression design allows PostgreSQL to preserve the original syntax for deparsing (displaying the query back to users) while using the properly typed and formatted expression during execution. The raw_expr is used when displaying the query structure, while formatted_expr takes precedence during actual evaluation.

This approach ensures that JSON operations can handle type coercion requirements from either explicit FORMAT clauses or implicit requirements from enclosing RETURNING clauses, while maintaining query readability and correctness.

## Parameters / Member Variables
- `type`: Standard NodeTag for node type identification
- `raw_expr`: Pointer to the original user-specified expression as written in the SQL
- `formatted_expr`: Pointer to the coerced expression that matches formatting requirements
- `format`: Pointer to JsonFormat structure specifying the JSON format, if FORMAT clause was specified

## Dependencies
- Functions called/Symbols referenced:
  - JsonFormat
  - Expr

- Called from (representative examples):
  - transformJsonValueExpr
  - transformJsonArrayConstructor  
  - transformJsonParseArg
  - transformJsonParseExpr
  - makeJsonValueExpr
  - makeJsonKeyValue
  - ExecInitExprRec
  - eval_const_expressions_mutator
  - get_rule_expr
  - exprType
  - exprTypmod
  - exprCollation
  - exprSetCollation
  - exprLocation

## Notes and Other Information
- The distinction between raw_expr and formatted_expr enables proper handling of type coercion while preserving original query semantics
- During deparsing with get_rule_expr(), the raw_expr is printed to maintain original syntax
- During evaluation, formatted_expr takes precedence for correct type handling
- This structure is fundamental to PostgreSQL's JSON expression processing pipeline
- Located in src/include/nodes/primnodes.h:1680-1686