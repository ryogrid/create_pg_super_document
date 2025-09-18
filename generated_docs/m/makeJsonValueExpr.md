# makeJsonValueExpr

## Location
[src/backend/nodes/makefuncs.c:910-926](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/makefuncs.c#L910-L926)

## Overview
Creates a JsonValueExpr node for representing JSON value expressions in PostgreSQL's SQL/JSON query processing.

## Definition


## Detailed Description
The  function is a constructor function that creates and initializes a  node. This node type is used in PostgreSQL's implementation of SQL/JSON functionality to represent expressions that produce JSON values. The function allocates memory for a new  structure using the PostgreSQL node system and sets up its three primary components: the raw expression, formatted expression, and format specification.

## Parameters / Member Variables
- : The original, unprocessed expression that will produce a value
- : The expression after formatting has been applied
- : A JsonFormat structure specifying how the JSON value should be formatted

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (PostgreSQL node allocation macro)
  - JsonValueExpr (node type structure)
  - JsonFormat (format specification structure)
- Called from (representative examples):
  - [eval_const_expressions_mutator](../e/eval_const_expressions_mutator.md) (optimizer/util/clauses.c:2934)
  - [transformJsonArrayQueryConstructor](../t/transformJsonArrayQueryConstructor.md) (parser/parse_expr.c:3785)
  - [transformJsonParseArg](../t/transformJsonParseArg.md) (parser/parse_expr.c:4057)
  - [transformJsonTableColumn](../t/transformJsonTableColumn.md) (parser/parse_jsontable.c:416)

## Notes and Other Information
This function is part of PostgreSQL's SQL/JSON implementation and is typically used during the parsing and transformation phases of query processing. The JsonValueExpr node created by this function represents a value that can be used in JSON operations and queries. The function follows PostgreSQL's standard node creation pattern using the makeNode macro for memory allocation and type initialization.