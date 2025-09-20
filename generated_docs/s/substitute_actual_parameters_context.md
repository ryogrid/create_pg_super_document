# substitute_actual_parameters_context

## Location
[src/backend/optimizer/util/clauses.c:73-79](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L73-L79)

## Overview
A context structure used during parameter substitution in PostgreSQL to track arguments and their usage counts when replacing Param nodes with actual parameter values in expression trees.

## Definition

```c
typedef struct
{
	int			nargs;
	List	   *args;
	int			sublevels_up;
} substitute_actual_srf_parameters_context;
```
## Detailed Description
The substitute_actual_parameters_context structure provides the necessary context information for the parameter substitution process in PostgreSQL's query optimization. This structure is used when transforming expression trees by replacing Param nodes with their corresponding actual parameter values. The context tracks both the available arguments and maintains usage statistics for each parameter, which is essential for optimization decisions and parameter validation during the substitution process.

## Parameters / Member Variables
- : Integer specifying the total number of arguments available for substitution
- : List containing the actual parameter values/expressions to substitute for Param nodes
- : Integer array tracking the usage count of each parameter (indexed by parameter ID minus 1)

## Dependencies
- Functions called/Symbols referenced:
  - [List](../L/List.md) (PostgreSQL list structure)
  - int (integer type)
- Called from (representative examples):
  - [substitute_actual_parameters](substitute_actual_parameters.md)
  - [substitute_actual_parameters_mutator](substitute_actual_parameters_mutator.md)

## Notes and Other Information
This context structure is crucial for function inlining and parameter substitution operations in the PostgreSQL optimizer. The usecounts array serves dual purposes: parameter validation (ensuring parameter IDs are within valid range) and usage tracking (for optimization decisions). The substitution process expects PARAM_EXTERN parameter kinds and validates parameter IDs against the nargs limit. Each successful parameter substitution increments the corresponding usage count, providing valuable information for subsequent optimization passes.