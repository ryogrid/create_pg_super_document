# assign_param_for_placeholdervar

## Location
src/backend/optimizer/util/paramassign.c: 149 - 196

## Overview
Selects a PARAM_EXEC number to identify the given PlaceHolderVar as a parameter for the current subquery and records the need for the PHV in the proper upper-level root->plan_params.

## Definition


## Detailed Description
This function is analogous to assign_param_for_var but specifically handles PlaceHolderVar nodes instead of Var nodes. PlaceHolderVars are special constructs used in PostgreSQL's optimizer to represent expressions that need to be evaluated at specific query levels, particularly in complex joins and subqueries.

The function navigates up the planner hierarchy to find the appropriate query level where the PlaceHolderVar belongs, then searches for an existing matching PlannerParamItem based on the PHV's unique identifier (phid). If no match is found, it creates a new parameter entry. The function uses IncrementVarSublevelsUp to adjust the PHV's level references and ensures the phlevelsup is set to 0 for the parameterized version.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context for the current query level
- : PlaceHolderVar node representing a placeholder expression that needs to be parameterized

## Dependencies
- Functions called/Symbols referenced:
  - [PlaceHolderVar](../P/PlaceHolderVar.md) (structure handling)
  - PlannerParamItem (structure creation)
  - copyObject (deep copy of the PlaceHolderVar node)
  - [IncrementVarSublevelsUp](../I/IncrementVarSublevelsUp.md) (adjust variable level references)
  - makeNode (node creation)
  - lappend_oid (append OID to list)
  - exprType (get expression type)
- Called from (representative examples):
  - [replace_outer_placeholdervar](../r/replace_outer_placeholdervar.md)

## Notes and Other Information
- The function assumes that comparing PHIDs (PlaceHolderVar IDs) is sufficient for matching
- Uses IncrementVarSublevelsUp with negative phlevelsup to adjust level references
- Includes an assertion to ensure phlevelsup is 0 after adjustment  
- The parameter type is determined by calling exprType on the PHV's expression (phexpr)
- This is a static function within paramassign.c, used internally for PlaceHolderVar parameter assignment
- PlaceHolderVars are more complex than regular Vars as they can contain arbitrary expressions