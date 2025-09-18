# generate_new_exec_param

## Location
src/backend/optimizer/util/paramassign.c: 637 - 663

## Overview
Generates a new execution parameter (Param node) with a unique ID that will not conflict with any existing parameters in the query plan.

## Definition
Param *generate_new_exec_param(PlannerInfo *root, Oid paramtype, int32 paramtypmod, Oid paramcollation)

## Detailed Description
This function creates a new Param node of kind PARAM_EXEC, which represents runtime parameters used for subplan outputs or NestLoop parameter passing. The function ensures uniqueness by assigning the parameter ID based on the current length of the paramExecTypes list, effectively using the next available slot.

The function automatically registers the parameter type in the global paramExecTypes list, which is essential for proper slot allocation during execution. Unlike user-supplied parameters, these execution parameters do not require a corresponding PlannerParamItem since they are internally generated and managed.

The created parameter has its location set to -1, indicating it is not associated with any specific source code location in the original query text.

## Parameters / Member Variables
- : PlannerInfo structure containing the global planning context
- : OID of the parameter's data type
- : Type modifier for the parameter (e.g., varchar length)
- : OID of the parameter's collation

## Dependencies
- Functions called/Symbols referenced:
  - Param
  - makeNode
  - PARAM_EXEC
  - lappend_oid
- Called from (representative examples):
  - build_subplan
  - generate_subquery_params
  - convert_EXISTS_to_ANY
  - SS_make_initplan_output_param
  - replace_nestloop_param_var
  - replace_nestloop_param_placeholdervar

## Notes and Other Information
This function is widely used throughout the optimizer for creating internal parameters. It is particularly important for subplan implementation and NestLoop parameterization. The automatic ID assignment ensures no conflicts with existing parameters, and the registration in paramExecTypes ensures proper runtime slot allocation.