# get_compatible_hash_operators

## Location
src/backend/utils/cache/lsyscache.c: 410 - 509

## Overview
Retrieves hash equality operators compatible with a given operator, operating on its left-hand side and/or right-hand side datatypes.

## Definition


## Detailed Description
This function finds hash equality operators that are compatible with the input operator but operate on specific datatypes. It's particularly useful for cross-type operators where the left and right operand types differ, requiring separate single-type hash operators for each side.

The function searches pg_amop for hash operator family registrations of the input operator as an equality operator (HTEqualStrategyNumber). For cross-type operators, it then locates the corresponding single-type equality operators for the left and/or right operand types using get_opfamily_member.

If the input operator is already single-type (left and right types are the same), both output parameters receive the same operator OID. The function ensures atomic success/failure - if it cannot find operators for all requested sides, it resets the outputs and continues searching other operator families.

## Parameters / Member Variables
- : The OID of the input operator to find compatible hash operators for
- : Optional output parameter for the left-hand side compatible operator (can be NULL)
- : Optional output parameter for the right-hand side compatible operator (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheList1
  - ReleaseSysCacheList
  - get_opfamily_member
  - HTEqualStrategyNumber
  - Form_pg_amop
  - CatCList
- Called from (representative examples):
  - ExecInitSubPlan
  - create_unique_plan

## Notes and Other Information
- Returns true if able to find all requested operators, false otherwise
- Output parameters are initialized to InvalidOid on failure
- Only considers operators registered for the hash access method
- Handles both single-type and cross-type operators appropriately
- If multiple hash operator families contain the operator, uses the first valid match found
- Essential for hash join planning and subplan execution where hash compatibility is required