# replace_outer_placeholdervar

## Location
[src/backend/optimizer/util/paramassign.c:197-223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/paramassign.c#L197-L223)

## Overview
Generates a Param node to replace the given PlaceHolderVar which is expected to have phlevelsup > 0, and records the need for the PHV in the proper upper-level root->plan_params.

## Definition


## Detailed Description
This function is the PlaceHolderVar equivalent of replace_outer_var, handling the conversion of PlaceHolderVar nodes into Param nodes for parameter passing between query levels. PlaceHolderVars represent expressions that need to be computed at specific levels in the query tree, and this function enables their parameterization for use in correlated subqueries.

The function validates that the PlaceHolderVar is from an outer level (phlevelsup > 0), then calls assign_param_for_placeholdervar to obtain or create a parameter ID. It constructs a new Param node with PARAM_EXEC kind, determining the type information by examining the PHV's expression rather than using fixed type fields like with regular Vars.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context for the current query level
- : PlaceHolderVar node representing a placeholder expression from an outer query level (phlevelsup > 0)

## Dependencies
- Functions called/Symbols referenced:
  - [PlaceHolderVar](../P/PlaceHolderVar.md) (structure handling)
  - Param (node structure)
  - [assign_param_for_placeholdervar](../a/assign_param_for_placeholdervar.md) (to get parameter ID)
  - makeNode (node creation)
  - PARAM_EXEC (parameter type constant)
  - exprType (get expression type)
  - exprTypmod (get expression type modifier)
  - [exprCollation](../e/exprCollation.md) (get expression collation)
- Called from (representative examples):
  - [replace_correlation_vars_mutator](replace_correlation_vars_mutator.md)
  - PARAMASSIGN_H (header file reference)

## Notes and Other Information
- The function includes an assertion that phv->phlevelsup > 0 and phv->phlevelsup < root->query_level
- Unlike replace_outer_var, this function determines type information dynamically from the PHV's expression using exprType, exprTypmod, and exprCollation
- The location field is set to -1, indicating no specific source location
- The resulting Param node has paramkind set to PARAM_EXEC for execution-time parameter passing
- This function is declared in optimizer/paramassign.h and is part of the public interface for parameter assignment
- PlaceHolderVars are more complex than Vars as they contain arbitrary expressions that may have different types