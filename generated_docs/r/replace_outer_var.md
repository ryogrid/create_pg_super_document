# replace_outer_var

## Location
[src/backend/optimizer/util/paramassign.c:120-148](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/paramassign.c#L120-L148)

## Overview
Generates a Param node to replace the given Var which is expected to have varlevelsup > 0, and records the need for the Var in the proper upper-level root->plan_params.

## Definition


## Detailed Description
This function is a key component of PostgreSQL's parameter assignment mechanism for handling correlated subqueries. It takes a Var node that references a column from an outer query level (indicated by varlevelsup > 0) and converts it into a Param node that can be used to pass the value from the outer query to the inner query during execution.

The function first validates that the Var is indeed from an outer level, then calls assign_param_for_var to get or create a parameter ID. It then constructs a new Param node with PARAM_EXEC kind, copying the relevant type information from the original Var. This transformation is essential for executing correlated subqueries efficiently.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context for the current query level
- : Var node representing a column reference from an outer query level (varlevelsup > 0)

## Dependencies
- Functions called/Symbols referenced:
  - Param (node structure)
  - [assign_param_for_var](../a/assign_param_for_var.md) (to get parameter ID)
  - makeNode (node creation)
  - PARAM_EXEC (parameter type constant)
- Called from (representative examples):
  - [replace_correlation_vars_mutator](replace_correlation_vars_mutator.md)
  - PARAMASSIGN_H (header file reference)

## Notes and Other Information
- The function includes an assertion that var->varlevelsup > 0 and var->varlevelsup < root->query_level
- The resulting Param node has paramkind set to PARAM_EXEC, indicating it's an execution-time parameter
- All type-related information (type, typmod, collation) is copied from the original Var
- The location information is preserved for error reporting purposes
- This function is declared in optimizer/paramassign.h and is part of the public interface for parameter assignment