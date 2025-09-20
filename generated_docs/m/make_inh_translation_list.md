# make_inh_translation_list

## Location
[src/backend/optimizer/util/appendinfo.c:80-195](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/appendinfo.c#L80-L195)

## Overview
Builds the translation mapping between parent and child relation columns for inheritance hierarchies, creating both forward and reverse translation structures.

## Definition

```c
static void
make_inh_translation_list(Relation oldrelation, Relation newrelation,
						  Index newvarno,
						  AppendRelInfo *appinfo)
```
## Detailed Description
This function constructs the essential column mapping infrastructure needed for inheritance processing. It creates a list of Var nodes that translate parent table references to child table references, and a reverse-mapping array that maps child columns back to their parent equivalents. The function handles column name matching, type validation, and deals with dropped columns and schema differences between parent and child relations. It performs type and collation verification to ensure inheritance consistency.

## Parameters / Member Variables
- : The parent relation (source of the translation)
- : The child relation (target of the translation)  
- : Range table index for the new (child) relation
- : AppendRelInfo structure to populate with translation data

## Dependencies
- Functions called/Symbols referenced:
  - makeVar (creates Var nodes for column references)
  - [SearchSysCacheAttName](../S/SearchSysCacheAttName.md) (looks up attributes by name)
  - RelationGetDescr (gets relation tuple descriptor)
  - TupleDescAttr (accesses tuple descriptor attributes)
  - [palloc0](../p/palloc0.md) (allocates zeroed memory)
- Called from (representative examples):
  - [make_append_rel_info](make_append_rel_info.md)

## Notes and Other Information
- Handles the special case where parent and child are the same relation (self-inheritance)
- Uses an optimization to check sequential columns first before falling back to syscache lookups
- Validates type and collation compatibility between matching parent-child columns
- Creates a reverse-translation array with 1-based indexing (0 means no match)
- Properly handles dropped columns by inserting NULL entries in the translation list