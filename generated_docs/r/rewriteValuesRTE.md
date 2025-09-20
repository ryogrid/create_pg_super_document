# rewriteValuesRTE

## Location
[src/backend/rewrite/rewriteHandler.c:1403-1587](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L1403-L1587)

## Overview
Handles DEFAULT value replacement in VALUES RTEs during INSERT statement rewriting, replacing DEFAULT items with appropriate default expressions or NULL values.

## Definition

```c
static bool
rewriteValuesRTE(Query *parsetree, RangeTblEntry *rte, int rti,
				 Relation target_relation,
				 Bitmapset *unused_cols)
```
## Detailed Description
This function processes INSERT ... VALUES statements with multiple VALUES lists (VALUES RTE) and replaces any DEFAULT items with the appropriate default expressions. The function handles different scenarios based on the target relation type:

- For auto-updatable views: DEFAULT items are replaced with the view's default if available, otherwise left untouched for the underlying base relation to handle
- For other relation types (including rule- and trigger-updatable views): All DEFAULT items are replaced, setting to NULL if no default exists
- For columns in unused_cols: DEFAULT items are explicitly set to NULL regardless of relation type

The function performs optimization by first scanning for DEFAULT placeholders to avoid unnecessary processing if none exist.

## Parameters / Member Variables
- : The INSERT query being rewritten
- : The VALUES range table entry containing the VALUES lists
- : Range table index of the VALUES RTE
- : The target relation for the INSERT operation
- : Bitmapset of column numbers that are no longer used in the targetlist

## Dependencies
- Functions called/Symbols referenced:
  - [searchForDefault](../s/searchForDefault.md)
  - [matchLocks](../m/matchLocks.md)
  - [view_has_instead_trigger](../v/view_has_instead_trigger.md)
  - [build_column_default](../b/build_column_default.md)
  - [makeNullConst](../m/makeNullConst.md)
  - [coerce_null_to_domain](../c/coerce_null_to_domain.md)
  - [bms_is_member](../b/bms_is_member.md)
- Called from:
  - [RewriteQuery](../R/RewriteQuery.md)

## Notes and Other Information
- Only processes INSERT commands with VALUES RTEs
- Returns true if all DEFAULT items were replaced, false if some were left untouched (auto-updatable views)
- Handles subscripted or field assignment targetlist entries from already-replaced DEFAULT items in recursive calls
- Performs validation to ensure DEFAULT items only appear in appropriate contexts
- Uses expensive list rebuilding only when DEFAULT placeholders are actually present