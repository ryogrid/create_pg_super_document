# set_foreign_rel_properties

## Location
src/backend/optimizer/util/relnode.c: 589 - 626

## Overview
Sets up foreign-join fields for a join relation when both outer and inner relations are foreign tables (or joins) belonging to the same server and assigned to the same user.

## Definition
static void set_foreign_rel_properties(RelOptInfo *joinrel, RelOptInfo *outer_rel, RelOptInfo *inner_rel)

## Detailed Description
This static function determines whether a join relation can be pushed down to a foreign server by examining the foreign data wrapper properties of its constituent relations. The function only sets foreign join properties when both relations belong to the same foreign server and have compatible user access permissions.

The function handles three scenarios for user permission compatibility: (1) Both relations have identical userids, (2) Inner relation has zero userid (current user) and outer relation has explicit userid matching current user, (3) Outer relation has zero userid (current user) and inner relation has explicit userid matching current user. In cases 2 and 3, the useridiscurrent field is set to true, indicating that pushdown is only valid for the current user. When foreign join properties are successfully set, the function copies the serverid, userid, useridiscurrent flag, and fdwroutine from the appropriate source relation to enable GetForeignJoinPaths to be called later.

## Parameters / Member Variables
- : The join RelOptInfo structure to configure with foreign join properties
- : The outer relation of the join
- : The inner relation of the join

## Dependencies
- Functions called/Symbols referenced:
  - OidIsValid (checks if an OID is valid)
  - GetUserId (gets current user ID)
- Called from (representative examples):
  - build_join_rel
  - build_child_join_rel

## Notes and Other Information
- This is a static function, only used internally within relnode.c
- Sets useridiscurrent to true when user ID assumptions are made for current user
- If conditions are not met, foreign join fields remain invalid, preventing GetForeignJoinPaths from being called
- Enables foreign join pushdown optimization when both relations use the same FDW server
- Ensures proper access permission checking for foreign joins
- Located in src/backend/optimizer/util/relnode.c:589-626