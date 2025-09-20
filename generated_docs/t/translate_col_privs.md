# translate_col_privs

## Location
[src/backend/optimizer/util/inherit.c:710-759](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/inherit.c#L710-L759)

## Overview
Translates a bitmapset representing per-column privileges from a parent relation's attribute numbering to the child relation's attribute numbering in inheritance hierarchies.

## Definition

```c
static Bitmapset *
translate_col_privs(const Bitmapset *parent_privs,
					List *translated_vars)
```
## Detailed Description
This static function performs column privilege translation between parent and child relations in inheritance hierarchies. The translation is necessary because parent and child relations may have different attribute numbering due to differences in column layout. Key aspects include:

1. **System Attribute Handling**: System attributes (negative attribute numbers) maintain the same numbers across all tables, so they are copied directly without translation.

2. **Whole-Row Reference Special Case**: When the parent has a whole-row reference, instead of translating it to a child whole-row reference (which would require permissions on all child columns), the function sets per-column bits for all inherited columns only. This provides more appropriate permission checking.

3. **Regular Attribute Translation**: User-defined attributes (positive numbers) are translated using the translated_vars list, which maps parent column positions to child Var nodes. Dropped columns (represented by NULL entries) are skipped.

4. **Privilege Propagation**: For each inherited column, if the parent has privileges or if there's a whole-row reference, the corresponding child column receives privileges.

The function ensures proper security enforcement while accommodating the structural differences between parent and child tables.

## Parameters / Member Variables
- : Bitmapset of column privileges in parent relation's attribute numbering
- : List of Var nodes mapping parent columns to child columns (NULL for dropped columns)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_member](../b/bms_is_member.md)
  - [bms_add_member](../b/bms_add_member.md)
  - FirstLowInvalidHeapAttributeNumber
  - InvalidAttrNumber
  - lfirst_node macro
- Called from (representative examples):
  - [expand_partitioned_rtentry](../e/expand_partitioned_rtentry.md)
  - [translate_col_privs_multilevel](translate_col_privs_multilevel.md)

## Notes and Other Information
- The function is static and used internally within the inheritance handling subsystem
- Handles both positive (user) and negative (system) attribute numbers differently
- Uses FirstLowInvalidHeapAttributeNumber offset for proper bitmapset indexing since attribute numbers can be negative
- Whole-row references are intentionally not translated to avoid overly strict permission requirements
- Dropped columns are safely ignored during translation by checking for NULL vars
- Essential for maintaining proper column-level security in inheritance and partitioning scenarios