# get_join_variables

## Location
src/backend/utils/adt/selfuncs.c: 4956 - 4983

## Overview
Analyzes both sides of a join clause by applying examine_variable() to each argument and determines the orientation (normal or reversed) of the join relative to the SpecialJoinInfo structure.

## Definition


## Detailed Description
This function is essential for join selectivity estimation in PostgreSQL's query planner. It processes both arguments of a join clause by examining each side using  to extract statistical information about the variables involved. The function then determines whether the join clause follows the expected orientation relative to the join structure.

The function distinguishes between "normal" and "reversed" join clauses. A join is considered "normal" if it follows the pattern "lhs_var OP rhs_var" (left-hand side variable operator right-hand side variable), and "reversed" if it follows "rhs_var OP lhs_var". This orientation information is crucial for the planner to correctly apply join selectivity estimates and understand the relationship between the join clause and the overall join structure defined in SpecialJoinInfo.

The determination of join orientation is made by checking which side of the join each variable belongs to, using bitmap subset operations to compare the relations involved in each variable against the left-hand and right-hand sides defined in the SpecialJoinInfo structure.

## Parameters / Member Variables
- : Pointer to PlannerInfo structure containing planner context and information
- : List containing the two arguments of the join operator clause
- : Pointer to SpecialJoinInfo structure containing information about the join structure
- : Output parameter that receives statistical information about the first (left) variable
- : Output parameter that receives statistical information about the second (right) variable
- : Output parameter set to true if the join clause is in reversed orientation, false if normal

## Dependencies
- Functions called/Symbols referenced:
  - list_length
  - elog
  - linitial
  - lsecond
  - examine_variable
  - bms_is_subset
- Called from (representative examples):
  - eqjoinsel
  - neqjoinsel
  - networkjoinsel

## Notes and Other Information
- The function expects exactly two arguments for the join operator and will throw an ERROR if this condition is not met
- When examining variables, the function passes 0 as the varRelid parameter, meaning variables from all relations are considered as variables (not pseudoconstants)
- The join orientation detection uses bitmap set operations to determine which side of the join each variable belongs to
- In complicated cases where the orientation cannot be definitively determined, the function defaults to considering the join as normal (not reversed)
- This function is primarily used by join selectivity estimation functions like eqjoinsel and neqjoinsel to understand the structure of join conditions