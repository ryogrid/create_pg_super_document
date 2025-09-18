# transformRangeTableSample

## Location
[src/backend/parser/parse_clause.c:910-1012](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L910-L1012)

## Overview
Transforms a TABLESAMPLE clause into a TableSampleClause node, validating the sampling method, processing arguments with type coercion, and handling REPEATABLE specifications.

## Definition
static TableSampleClause *
transformRangeTableSample(ParseState *pstate, RangeTableSample *rts)

## Detailed Description
The transformRangeTableSample function handles the transformation of TABLESAMPLE clauses used for statistical sampling of table data. The function validates the tablesample method by looking up its handler function (which must accept one INTERNAL argument and return tsm_handler type), retrieves the TsmRoutine to get parameter type information, transforms and type-coerces all method arguments according to the expected parameter types, and processes the optional REPEATABLE clause (if supported by the method). The function ensures that the correct number of arguments are provided and that all expressions are properly transformed and assigned collations.

## Parameters / Member Variables
- pstate: ParseState structure containing the current parsing context and state information
- rts: RangeTableSample structure representing the raw TABLESAMPLE clause to be transformed, including method name, arguments, and optional REPEATABLE clause

## Dependencies
- Functions called/Symbols referenced:
  - [LookupFuncName](../L/LookupFuncName.md)
  - [get_func_rettype](../g/get_func_rettype.md)  
  - [GetTsmRoutine](../G/GetTsmRoutine.md)
  - makeNode
  - [transformExpr](transformExpr.md)
  - [coerce_to_specific_type](../c/coerce_to_specific_type.md)
  - [assign_expr_collations](../a/assign_expr_collations.md)
  - [NameListToString](../N/NameListToString.md)
  - TSM_HANDLEROID
  - EXPR_KIND_FROM_FUNCTION
  - FLOAT8OID
- Called from (representative examples):
  - [transformFromClauseItem](transformFromClauseItem.md)

## Notes and Other Information
- Tablesample method names are looked up as functions with specific signature: one INTERNAL argument returning tsm_handler type
- Schema qualification is allowed for tablesample method names to resolve ambiguity
- The function validates that the handler function returns TSM_HANDLEROID type
- Argument count validation ensures the provided arguments match the method's expected parameter count
- All arguments are transformed using EXPR_KIND_FROM_FUNCTION context and coerced to the expected parameter types
- REPEATABLE clause is optional and only supported by methods that set repeatable_across_queries = true
- REPEATABLE argument is always coerced to FLOAT8OID (double precision) type
- Collation assignment is performed immediately since assign_query_collations() doesn't examine RTE substructure
- Error messages distinguish between 'method does not exist' vs 'function does not exist' for better user experience
- The resulting TableSampleClause contains the handler OID, transformed arguments, and optional repeatable expression