# get_translated_update_targetlist

## Location
src/backend/optimizer/util/appendinfo.c: 690 - 732

## Overview
Retrieves the processed target list of an UPDATE query, translated as needed to match a specific child target relation in inheritance or partitioning scenarios.

## Definition
```c
void get_translated_update_targetlist(PlannerInfo *root, Index relid, List **processed_tlist, List **update_colnos)
```

## Detailed Description
This function provides the processed target list for UPDATE operations, handling both simple cases and complex inheritance/partitioning scenarios. For non-inheritance cases where the target relation matches the original result relation, it simply returns copies of the stored processed_tlist and update_colnos. For inheritance cases, it performs comprehensive translation using multi-level adjustment functions to ensure that target list expressions and column numbers are properly mapped to the specific child relation.

The function is specifically designed for UPDATE commands and includes appropriate assertions to enforce this constraint. It handles the complexity of translating both the target list expressions and the column numbers separately, since the resnos in the processed target list cannot be relied upon for column identification in inheritance scenarios.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planning context and query information
- `relid`: Index of the target relation for which the translated target list is needed
- `processed_tlist`: Output parameter for the translated processed target list
- `update_colnos`: Optional output parameter for the translated update column numbers

## Dependencies
- Functions called/Symbols referenced:
  - CMD_UPDATE (command type constant)
  - copyObject (creates deep copies of node trees)
  - bms_is_member (checks if relation ID is in result relations set)
  - adjust_appendrel_attrs_multilevel (translates expressions for inheritance)
  - find_base_rel (locates RelOptInfo for given relation ID)
  - adjust_inherited_attnums_multilevel (translates attribute numbers)
  - AppendRelInfo (structure type)
- Called from (representative examples):
  - (Referenced from header file, specific callers not shown)

## Notes and Other Information
- Specifically designed for UPDATE commands only, enforced by assertion
- Handles both inheritance and non-inheritance cases with appropriate optimizations
- Returns copies of data structures to allow safe modification by callers
- Essential for PostgreSQL's handling of UPDATE operations on partitioned and inherited tables
- Separates translation of target list expressions and column numbers for accuracy
- The resnos in processed_tlist are explicitly noted as unreliable for column identification