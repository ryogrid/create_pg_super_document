# adjust_relid_set

## Location
src/backend/rewrite/rewriteManip.c: 738 - 773

## Overview
A static utility function that substitutes one relation ID with another in a bitmap set of relation IDs (Relids).

## Definition
```c
static Relids adjust_relid_set(Relids relids, int oldrelid, int newrelid)
```

## Detailed Description
This function provides a safe and efficient way to update relation ID sets during query rewriting operations. It checks if the old relation ID exists in the given set, and if so, creates a modified copy of the set with the old ID removed and the new ID added. 

The function includes special handling for PostgreSQL's special variable numbers (like INDEX_VAR) that should not be processed through the normal bitmap operations. It ensures that only valid relation IDs are processed while tolerating special varnos that may be passed by extensions.

The function is designed to be non-destructive, returning either the original set (if no change is needed) or a new modified copy, following PostgreSQL's general pattern of avoiding in-place modifications.

## Parameters / Member Variables
- `relids`: The input bitmap set of relation IDs to potentially modify
- `oldrelid`: The relation ID to be replaced (if present in the set)
- `newrelid`: The new relation ID to substitute for the old one

## Dependencies
- Functions called/Symbols referenced:
  - IS_SPECIAL_VARNO (macro to check for special variable numbers)
  - [bms_is_member](../b/bms_is_member.md) (bitmap set membership test)
  - [bms_copy](../b/bms_copy.md) (create a copy of a bitmap set)
  - [bms_del_member](../b/bms_del_member.md) (remove a member from bitmap set)
  - [bms_add_member](../b/bms_add_member.md) (add a member to bitmap set)
- Called from (representative examples):
  - [ChangeVarNodes_walker](../C/ChangeVarNodes_walker.md) (for updating varnullingrels and phnullingrels)

## Notes and Other Information
- This is a static function used only within rewriteManip.c
- Handles the common pattern of needing to update relation ID sets during query transformation
- Includes safety checks for special variable numbers that extensions might pass
- Returns the original set unchanged if the old relation ID is not present
- Uses PostgreSQL's bitmap set (bms) infrastructure for efficient set operations
- The function creates a modifiable copy only when changes are actually needed, optimizing for the common case where no modification is required
- Primarily used for updating nulling relations sets in Var and PlaceHolderVar nodes during range table index changes