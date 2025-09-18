# get_aggregate_argtypes

## Location
[src/backend/parser/parse_agg.c:1906-1931](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_agg.c#L1906-L1931)

## Overview
Extracts the actual datatypes of input arguments passed to an aggregate function call from an Aggref node.

## Definition


## Detailed Description
This function identifies the specific datatypes passed to an aggregate call by examining an Aggref node. It extracts the actual datatypes of the input arguments and reports them in a way that matches the aggregate's declaration. The function handles the nuances of different aggregate types:

- For plain aggregates: ORDER BY columns are ignored in the argument type extraction
- For ordered-set aggregates: Both direct and aggregated arguments are reported

The function iterates through the  list in the Aggref node and populates the provided array with the corresponding Oid values representing the datatypes.

## Parameters / Member Variables
- : Pointer to an Aggref node containing aggregate function call information
- : Output array of Oid values to store the extracted argument datatypes (must be allocated with length FUNC_MAX_ARGS)

## Dependencies
- Functions called/Symbols referenced:
  -  (struct type)
  -  (constant defining maximum function arguments)
  -  (for validation)
  -  (for extracting Oid values from list cells)
- Called from (representative examples):
  -  (in nodeAgg.c)
  -  (in prepagg.c)
  -  (in ruleutils.c)

## Notes and Other Information
- The function includes an assertion to ensure the number of argument types doesn't exceed FUNC_MAX_ARGS
- Returns the total number of actual arguments processed
- The inputTypes array must be pre-allocated by the caller with sufficient space (FUNC_MAX_ARGS elements)
- This function is part of the aggregate function processing pipeline in PostgreSQL's parser subsystem