# query_contains_extern_params

## Location
src/backend/parser/parse_param.c: 330 - 337

## Overview
Determines whether a fully-parsed query tree contains any external parameters (PARAM_EXTERN).

## Definition


## Detailed Description
This function provides a simple boolean check to determine if a given query tree contains any external parameters. It serves as a convenience wrapper around the query tree walking mechanism, using query_contains_extern_params_walker to traverse the entire query structure and detect the presence of PARAM_EXTERN parameter nodes.

The function is useful for determining whether a query has external parameter dependencies before execution or for optimization decisions that depend on parameter presence.

## Parameters / Member Variables
- : Query tree to be examined for external parameters

## Dependencies
- Functions called/Symbols referenced:
  - query_tree_walker
  - [query_contains_extern_params_walker](query_contains_extern_params_walker.md)
- Called from (representative examples):
  - [transformCreateTableAsStmt](../t/transformCreateTableAsStmt.md)

## Notes and Other Information
- Returns true if any PARAM_EXTERN parameters are found, false otherwise
- Located in src/backend/parser/parse_param.c:330-337
- This is a utility function that simplifies parameter detection for callers
- Uses the standard PostgreSQL tree walking pattern with a specialized walker function
- Part of the parameter analysis infrastructure used throughout the parser