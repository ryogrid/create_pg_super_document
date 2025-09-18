# expand_insert_targetlist

## Location
src/backend/optimizer/prep/preptlist.c: 382 - 525

## Overview
Expands an INSERT targetlist to include entries for missing table attributes and ensures non-junk attributes appear in proper field order to match the target relation's structure.

## Definition
```c
static List *expand_insert_targetlist(PlannerInfo *root, List *tlist, Relation rel)
```

## Detailed Description
The `expand_insert_targetlist` function takes a parser-generated targetlist for an INSERT statement and transforms it to match exactly the structure expected by the executor. The executor requires that the targetlist contain entries for every attribute in the target table, in the exact order they appear in the table definition.

The function scans through each attribute in the target relation and either:
1. Uses the existing targetlist entry if one exists for that attribute
2. Creates a new NULL-valued targetlist entry if the attribute is missing from the original targetlist

Special handling is provided for different column types:
- **Dropped columns**: Inserts a NULL constant with INT4 type (since the original datatype may no longer exist)
- **Generated columns**: Inserts a NULL of the base type without domain constraints to avoid errors
- **Normal columns**: Uses `coerce_null_to_domain` to create a properly-typed NULL that respects domain constraints

After processing all table attributes, any remaining resjunk (auxiliary) entries from the original targetlist are appended with properly renumbered resnos.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planning context and state
- `tlist`: Original targetlist from the parser
- `rel`: Target relation for the INSERT operation

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNumberOfAttributes
  - TupleDescAttr
  - [makeConst](../m/makeConst.md)
  - [getBaseTypeAndTypmod](../g/getBaseTypeAndTypmod.md)
  - [coerce_null_to_domain](../c/coerce_null_to_domain.md)
  - [eval_const_expressions](eval_const_expressions.md)
  - [makeTargetEntry](../m/makeTargetEntry.md)
  - [flatCopyTargetEntry](../f/flatCopyTargetEntry.md)
  - list_head
  - [lnext](../l/lnext.md)
- Called from (representative examples):
  - [preprocess_targetlist](../p/preprocess_targetlist.md) (src/backend/optimizer/prep/preptlist.c:107, 153)

## Notes and Other Information
This function is located in src/backend/optimizer/prep/preptlist.c:382-525 and is declared as static, meaning it's only used within the same file. It's a critical component for INSERT statement processing, ensuring that the executor receives a complete and correctly ordered targetlist. The function handles various PostgreSQL-specific features like dropped columns, generated columns, and domain constraints while maintaining compatibility with the executor's expectations.