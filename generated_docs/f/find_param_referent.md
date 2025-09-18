# find_param_referent

## Location
[src/backend/utils/adt/ruleutils.c:8162-8275](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L8162-L8275)

## Overview
Locates the referenced expression for a PARAM_EXEC parameter by searching through ancestor plan nodes (NestLoop and SubPlan) to find matching parameter definitions.

## Definition


## Detailed Description
This function implements parameter resolution for PARAM_EXEC parameters, which are used to pass values between different levels of a plan tree. These parameters are commonly created by NestLoop joins (to pass outer relation values to the inner side) and SubPlan nodes (to pass arguments into subqueries).

The function performs a systematic search through the ancestor plan tree:

1. **Parameter Type Filtering**: Only processes PARAM_EXEC parameters, as other parameter types (PARAM_EXTERN, etc.) are handled differently.

2. **Ancestor Traversal**: Walks up through the ancestors list in the deparse namespace, examining each ancestor plan node to find one that defines the target parameter.

3. **NestLoop Parameter Resolution**: 
   - Checks if the ancestor is a NestLoop and the current plan is its inner child
   - Searches through the NestLoop's nestParams list for a matching parameter ID
   - Returns the paramval expression if found

4. **SubPlan Parameter Resolution**:
   - For SubPlan ancestors, examines the parParam and args lists in parallel
   - Finds matching parameter IDs and returns the corresponding argument expression
   - Special handling for SubPlan context: must find the next non-SubPlan ancestor for proper variable evaluation context

5. **Context Management**: Sets output parameters (*dpns_p and *ancestor_cell_p) to enable proper push_ancestor_plan() calls by the caller.

The function maintains the invariant that parameters are resolved in their proper evaluation context, ensuring that variables in the resolved expressions are interpreted correctly.

## Parameters / Member Variables
- : The PARAM_EXEC parameter node whose referent needs to be found
- : Deparse context containing namespace stack with ancestor information
- : Output parameter - pointer to the deparse_namespace containing the referent
- : Output parameter - ListCell pointer for push_ancestor_plan context setup

## Dependencies
- Functions called/Symbols referenced:
  - linitial (list access)
  - innerPlan (plan tree navigation)
  - lfirst/lfirst_int (list cell access)
  - forboth (parallel list iteration)
  - for_each_cell/lnext (list traversal)
- Called from (representative examples):
  - [get_name_for_var_field](../g/get_name_for_var_field.md) (for RECORD type parameter resolution)
  - [get_parameter](../g/get_parameter.md) (for general parameter decompilation)

## Notes and Other Information
- Returns NULL if no referent can be found, indicating the parameter cannot be resolved
- Only handles PARAM_EXEC parameters; other parameter kinds are ignored
- NestLoop parameters are only passed to the inner child, never the outer child
- SubPlan argument resolution requires finding a non-SubPlan ancestor for proper variable context
- The function does not examine initPlan lists since initplans never have parParams
- Critical for proper decompilation of correlated subqueries and parameterized nested loops
- Output parameters must be used with push_ancestor_plan() to establish correct evaluation context
- Error checking ensures SubPlan nodes are never the outermost ancestor (which would be invalid)
- The ancestor traversal continues until a match is found or all ancestors are exhausted