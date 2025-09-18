# replace_outer_agg

## Location
src/backend/optimizer/util/paramassign.c: 224 - 269

## Overview
Generates a Param node to replace the given Aggref which is expected to have agglevelsup > 0, and records the need for the Aggref in the proper upper-level root->plan_params.

## Definition


## Detailed Description
This function handles the parameterization of aggregate function references (Aggref nodes) that belong to outer query levels. Unlike the deduplication strategies used for Vars and PlaceHolderVars, this function intentionally creates a new parameter slot every time, as indicated by the comment that it does not seem worthwhile to try to de-duplicate references to outer aggregates.

The function navigates up the planner hierarchy to find the query level where the aggregate belongs, then creates a copy of the Aggref and adjusts its level references using IncrementVarSublevelsUp. It then creates both a PlannerParamItem to track the parameter and a Param node to replace the original Aggref. The resulting parameter has type information derived directly from the aggregate's type fields.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context for the current query level
- : Aggref node representing an aggregate function reference from an outer query level (agglevelsup > 0)

## Dependencies
- Functions called/Symbols referenced:
  - Aggref (aggregate reference structure)
  - Param (parameter node structure)
  - PlannerParamItem (parameter item structure)
  - copyObject (deep copy of the Aggref node)
  - IncrementVarSublevelsUp (adjust variable level references)
  - makeNode (node creation)
  - lappend_oid (append OID to list)
  - PARAM_EXEC (parameter type constant)
- Called from (representative examples):
  - replace_correlation_vars_mutator
  - PARAMASSIGN_H (header file reference)

## Notes and Other Information
- The function includes an assertion that agg->agglevelsup > 0 and agg->agglevelsup < root->query_level
- Unlike other parameter assignment functions, this one does not attempt deduplication and creates a new parameter slot each time
- Uses IncrementVarSublevelsUp with negative agglevelsup to adjust level references
- Includes an assertion to ensure agglevelsup is 0 after adjustment
- The paramtypmod is set to -1, indicating no specific type modifier
- Type information (aggtype, aggcollid) is copied directly from the Aggref
- Location information is preserved for error reporting purposes
- This function is declared in optimizer/paramassign.h and is part of the public interface for parameter assignment