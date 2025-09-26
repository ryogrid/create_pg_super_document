# find_param_path_info

## Location
src/backend/optimizer/util/relnode.c: 1901 - 1921

## Overview
Search for an existing ParamPathInfo in a relation's cache that matches the given parameterization requirements.

## Definition


## Detailed Description
This utility function searches through a relation's list of cached ParamPathInfo structures (ppilist) to find one that matches the specified parameterization requirements. It performs a simple linear search comparing the required_outer parameter set with the ppi_req_outer field of each cached ParamPathInfo.

The function is used by the various get_*_parampathinfo functions to avoid creating duplicate ParamPathInfo structures for the same parameterization, ensuring consistent rowcount estimates and efficient memory usage.

## Parameters / Member Variables
- : RelOptInfo structure whose ppilist will be searched
- : Relids bitmap specifying the parameterization to search for

## Dependencies
- Functions called/Symbols referenced:
  - bms_equal
- Called from (representative examples):
  - get_baserel_parampathinfo
  - get_joinrel_parampathinfo
  - get_appendrel_parampathinfo
  - REPARAMETERIZE_CHILD_PATH_LIST

## Notes and Other Information
- Returns NULL if no matching ParamPathInfo is found
- Performs linear search through the relation's ppilist
- Used for caching optimization to avoid duplicate ParamPathInfo creation
- Simple but effective for the typically small number of parameterizations per relation
- The function is located in src/backend/optimizer/util/relnode.c:1901-1921