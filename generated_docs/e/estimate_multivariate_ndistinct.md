# estimate_multivariate_ndistinct

## Location
src/backend/utils/adt/selfuncs.c: 3967 - 4317

## Overview
Finds applicable multivariate ndistinct statistics for a given list of variables/expressions belonging to a relation and estimates the number of distinct values using the best matching statistics object.

## Definition


## Detailed Description
This function searches through extended statistics objects for the relation to find the most applicable multivariate ndistinct statistic that matches the given variables and expressions. It performs the following steps:

1. **Statistics Object Selection**: Iterates through all available extended statistics objects, filtering for STATS_EXT_NDISTINCT type and matching inheritance settings
2. **Matching Logic**: For each statistics object, counts how many variables and expressions from the input list match the statistics object's keys and expressions
3. **Best Match Selection**: Chooses the statistics object with the highest number of matching expressions, with variables as a tiebreaker
4. **Statistics Loading**: Loads the selected statistics object using statext_ndistinct_load
5. **Item Matching**: Finds the specific MVNDistinctItem within the statistics that exactly matches the combination of variables/expressions
6. **Output Construction**: Updates the ndistinct estimate and removes matched variables from the input varinfos list

The function handles both simple Var nodes and complex expressions, applying appropriate attribute number offsets to handle the internal representation of extended statistics.

## Parameters
- : PlannerInfo structure containing query planning context
- : RelOptInfo for the relation containing the statistics
- : Input/output list of GroupVarInfo structures representing variables/expressions (modified to remove matched items)
- : Output parameter for the estimated number of distinct values

## Dependencies
- Functions called:
  - planner_rt_fetch
  - bms_is_member
  - bms_add_member
  - bms_num_members
  - equal
  - statext_ndistinct_load
  - AttrNumberIsForUserDefinedAttr
- Called from:
  - estimate_num_groups (in selfuncs.c:3635)

## Notes and Other Information
- Returns true if a matching statistics object is found, false otherwise
- Requires at least two matching variables/expressions to consider a statistics object applicable
- Only processes user-defined attributes, ignoring system attributes
- The function includes logic to handle attribute number offsets when expressions are present in the statistics object
- Tie-breaking mechanism should be improved to use object names for stable outcomes
- The function assumes that ndistinct statistics include all combinations of attributes
- Extended statistics must match the inheritance setting of the range table entry