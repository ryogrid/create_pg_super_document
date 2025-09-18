# gin_triconsistent_jsonb_path

## Location
src/backend/utils/adt/jsonb_gin.c: 1272 - 1325

## Overview
This function implements the GIN triconsistent support function for JSONB path operations, providing ternary logic for index scans to determine whether specific keys might match query conditions before full tuple examination.

## Definition
Datum gin_triconsistent_jsonb_path(PG_FUNCTION_ARGS)

## Detailed Description
The gin_triconsistent_jsonb_path function is a GIN index access method support function that implements the triconsistent interface for JSONB path-based queries. It performs fast pre-filtering during index scans by evaluating whether combinations of index keys can potentially satisfy query conditions without examining actual tuples. The function supports multiple JSONB query strategies including containment (@>) and JSONPath predicate/existence operations (?? and @?). It returns ternary values (GIN_TRUE, GIN_FALSE, or GIN_MAYBE) to indicate whether a combination of keys definitely matches, definitely doesn't match, or might match the query condition.

For containment queries, the function implements conservative logic that never returns GIN_TRUE (always requiring recheck) but can eliminate impossible matches by returning GIN_FALSE when any required key is missing. For JSONPath queries, it delegates to execute_jsp_gin_node() to evaluate the compiled JSONPath expression against the available keys, then converts any GIN_TRUE result to GIN_MAYBE to force recheck verification.

## Parameters / Member Variables
- : Array of GinTernaryValue indicating availability of each extracted key in the current item
- : Strategy number identifying the query operator type (containment, JSONPath predicate, or JSONPath existence)  
- : The JSONB query value (commented out as unused in current implementation)
- : Number of extracted keys being checked
- : Additional data array containing compiled JSONPath information for path-based queries

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER
  - PG_GETARG_UINT16  
  - PG_GETARG_INT32
  - [execute_jsp_gin_node](../e/execute_jsp_gin_node.md)
  - PG_RETURN_GIN_TERNARY_VALUE
  - elog
- Types used:
  - GinTernaryValue
  - StrategyNumber  
  - JsonPathGinNode
- Constants:
  - JsonbContainsStrategyNumber
  - JsonbJsonpathPredicateStrategyNumber
  - JsonbJsonpathExistsStrategyNumber
  - GIN_MAYBE, GIN_TRUE, GIN_FALSE
- Called from (representative examples):
  - GIN index access method during query execution
  - Used as triconsistent support function in GIN operator class

## Notes and Other Information
The function never returns GIN_TRUE for any strategy, always requiring recheck in the main consistent function to ensure correctness. This conservative approach prevents false positives while still allowing the index to eliminate impossible matches. For JSONPath operations, the function relies on pre-compiled JSONPath expressions stored in extra_data to efficiently evaluate complex path predicates against the available index keys. The triconsistent interface is an optimization that can significantly reduce the number of heap tuple accesses during index scans by eliminating obviously non-matching key combinations early in the process.