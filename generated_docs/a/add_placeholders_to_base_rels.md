# add_placeholders_to_base_rels

## Location
[src/backend/optimizer/util/placeholder.c:329-372](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/placeholder.c#L329-L372)

## Overview
Adds required PlaceHolderVars to base relations' target lists when placeholders can be computed at a base relation and are needed above it.

## Definition
```c
void add_placeholders_to_base_rels(PlannerInfo *root)
```

## Detailed Description
This function identifies placeholders that can be computed at a base relation level and are needed at higher levels in the query plan, then adds them to the appropriate base relation's target list. It must be executed separately from fix_placeholder_input_needed_levels because join removal happens between these operations and can change the ph_eval_at sets.

The function iterates through all placeholders and checks if the evaluation can be done at a single base relation (using bms_get_singleton_member). If the placeholder is needed at levels beyond where it's evaluated, it's added to the base relation's target list. This ensures the placeholder value will be available when needed during query execution.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the placeholder list and relation information

## Dependencies
- Functions called/Symbols referenced:
  - [PlaceHolderInfo](../P/PlaceHolderInfo.md) (struct type for placeholder information)
  - [bms_get_singleton_member](../b/bms_get_singleton_member.md) (checks if a bitmap has exactly one member)
  - [bms_nonempty_difference](../b/bms_nonempty_difference.md) (checks for non-empty set difference)
  - [find_base_rel](../f/find_base_rel.md) (finds base relation by relation number)
  - copyObject (creates a copy of the placeholder variable)
- Called from (representative examples):
  - [query_planner](../q/query_planner.md) (src/backend/optimizer/plan/planmain.c:238)

## Notes and Other Information
- This function must be executed after fix_placeholder_input_needed_levels but before join relations are formed
- [Join](../J/Join.md) removal can occur between placeholder level fixing and this operation, which is why they must be separate steps
- The function includes an assertion that phnullingrels should be empty for scan-level computations, as values haven't been nulled by outer joins yet
- The function copies the PlaceHolderVar when adding it to the target list for safety
- Target list cost and width fields are updated later in the planning process
- Similar logic exists in add_placeholders_to_joinrel for handling join-level placeholder additions