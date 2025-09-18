# CombineRangeTables

## Location
src/backend/rewrite/rewriteManip.c: 351 - 388

## Overview
A utility function that merges range table entries (RTEs) and their associated permission information from a source into a destination, updating permission indexes accordingly.

## Definition


## Detailed Description
This function combines two range tables by appending the source range table entries to the destination range table. It also handles the associated permission information (RTEPermissionInfos) by merging the source permission list into the destination and updating the perminfoindex values in the source RTEs to correctly point to their new positions in the combined permission list. The function operates destructively on the destination lists, so callers should pass modifiable copies if the original lists need to be preserved.

## Parameters / Member Variables
- `dst_rtable`: Pointer to destination range table list (modified in-place)
- `dst_perminfos`: Pointer to destination permission information list (modified in-place)
- `src_rtable`: Source range table list to be merged
- `src_perminfos`: Source permission information list to be merged

## Dependencies
- Functions called/Symbols referenced:
  - list_length (to calculate offset)
  - list_concat (to merge lists)
  - lfirst_node (to access RangeTblEntry nodes)
  - foreach (list iteration macro)
- Called from (representative examples):
  - convert_EXISTS_sublink_to_join
  - pull_up_simple_subquery
  - pull_up_simple_union_all
  - rewriteRuleAction

## Notes and Other Information
- This function modifies both destination parameters destructively
- Permission indexes are adjusted by adding an offset equal to the original destination permission list length
- Used extensively in query rewriting and optimization phases
- Essential for combining range tables when pulling up subqueries or applying rewrite rules
- Only adjusts perminfoindex if it's greater than 0 (valid permission reference)
- The function maintains the integrity of RTE-to-permission mappings during table combination