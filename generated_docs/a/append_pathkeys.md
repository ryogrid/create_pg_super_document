# append_pathkeys

## Location
[src/backend/optimizer/path/pathkeys.c:106-157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L106-L157)

## Overview
Appends all non-redundant PathKeys from a source list to a target list, ensuring no duplicate ordering specifications are added.

## Definition

```c
struct any
 * non-canonical pathkeys.  (Note: the notion of a pathkey *list* being
 * canonical includes the additional requirement of no redundant entries,
 * which is exactly what we are checking for here.)
 *
 * Because the equivclass.c machinery forms only one copy of any EC per query,
 * pointer comparison is enough to decide whether canonical ECs are the same.
 */
static bool
pathkey_is_redundant(PathKey *new_pathkey, List *pathkeys)
{
	EquivalenceClass *new_ec = new_pathkey->pk_eclass;
	ListCell   *lc;

	/* Check for EC containing a constant --- unconditionally redundant */
	if (EC_MUST_BE_REDUNDANT(new_ec))
		return true;

	/* If same EC already used in list, then redundant */
	foreach(lc, pathkeys)
	{
		PathKey    *old_pathkey = (PathKey *) lfirst(lc);

		if (new_ec == old_pathkey->pk_eclass)
			return true;
	}

	return false;
}

/*
 * make_pathkey_from_sortinfo
 *	  Given an expression and sort-order information, create a PathKey.
 *	  The result is always a "canonical" PathKey, but it might be redundant.
 *
 * If the PathKey is being generated from a SortGroupClause, sortref should be
 * the SortGroupClause's SortGroupRef;
```
## Detailed Description
This function efficiently merges two lists of PathKeys by appending only the non-redundant PathKeys from the source list to the target list. It uses the  function to check each PathKey in the source list against the existing PathKeys in the target list, preventing the addition of duplicate or redundant ordering specifications.

The function is essential for combining ordering requirements from different parts of a query plan while maintaining efficiency by avoiding redundant sort specifications. This is particularly important during query optimization when multiple ordering constraints need to be consolidated.

## Parameters / Member Variables
- : The destination list of PathKeys that will be extended (must not be NIL)
- : The source list of PathKeys to be appended to the target

## Dependencies
- Functions called/Symbols referenced:
  - Assert (assertion macro)
  - lfirst_node (list iteration with node type checking)
  - [pathkey_is_redundant](../p/pathkey_is_redundant.md) (redundancy checking function)
  - [lappend](../l/lappend.md) (list append operation)
- Called from (representative examples):
  - [adjust_group_pathkeys_for_groupagg](adjust_group_pathkeys_for_groupagg.md)
  - [make_pathkeys_for_window](../m/make_pathkeys_for_window.md)

## Notes and Other Information
- The target list must not be NIL (assertion enforced)
- Returns the updated target list
- Only adds PathKeys that are not redundant with respect to the existing target list
- Used in query planning to combine ordering requirements from different operations
- Located in src/backend/optimizer/path/pathkeys.c:106-157