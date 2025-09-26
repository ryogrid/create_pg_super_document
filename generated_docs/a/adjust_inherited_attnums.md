# adjust_inherited_attnums

## Location
[src/backend/optimizer/util/appendinfo.c:628-661](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/appendinfo.c#L628-L661)

## Overview
Translates a list of parent attribute numbers to their corresponding child attribute numbers using AppendRelInfo context for inheritance relationships.

## Definition
```c
List *adjust_inherited_attnums(List *attnums, AppendRelInfo *context)
```

## Detailed Description
This function performs attribute number translation for inheritance scenarios by mapping parent table attribute numbers to their corresponding child table attribute numbers. It uses the translated_vars list from the AppendRelInfo structure to perform the mapping, ensuring that each parent attribute number corresponds to a valid Var node in the child relation.

The function includes comprehensive error checking to validate that each attribute number is within valid bounds and that the corresponding entry in the translated_vars list is a proper Var node. This is essential for maintaining data integrity when working with inherited table structures where column mappings may differ between parent and child relations.

## Parameters / Member Variables
- `attnums`: List of integer attribute numbers from the parent relation to be translated
- `context`: AppendRelInfo structure containing the parent-to-child attribute mapping information

## Dependencies
- Functions called/Symbols referenced:
  - [AppendRelInfo](../A/AppendRelInfo.md) (structure type)
  - lfirst_int (extracts integer from list cell)
  - [get_rel_name](../g/get_rel_name.md) (gets relation name for error messages)
  - [list_nth](../l/list_nth.md) (retrieves nth element from list)
  - [lappend_int](../l/lappend_int.md) (appends integer to list)
- Called from (representative examples):
  - [adjust_inherited_attnums_multilevel](adjust_inherited_attnums_multilevel.md)

## Notes and Other Information
- Specifically designed for inheritance cases, not UNION ALL operations (validated by assertion)
- Includes robust error checking with descriptive error messages referencing relation names
- Returns a new list containing the translated child attribute numbers
- Essential for PostgreSQL's inheritance system where parent and child tables may have different physical column layouts
- Uses 1-based attribute numbering convention consistent with PostgreSQL system catalogs