# estimate_multivariate_ndistinct

## Location
[src/backend/utils/adt/selfuncs.c:3967-4317](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L3967-L4317)

## Overview
Finds applicable multivariate ndistinct statistics for a given list of variables/expressions belonging to a relation and estimates the number of distinct values using the best matching statistics object.

## Definition

```c
struct the output values.
	 */
	if (stats)
	{
		int			i;
		List	   *newlist = NIL;
		MVNDistinctItem *item = NULL;
		ListCell   *lc2;
		Bitmapset  *matched = NULL;
		AttrNumber	attnum_offset;

		/*
		 * How much we need to offset the attnums? If there are no
		 * expressions, no offset is needed. Otherwise offset enough to move
		 * the lowest one (which is equal to number of expressions) to 1.
		 */
		if (matched_info->exprs)
			attnum_offset = (list_length(matched_info->exprs) + 1);
		else
			attnum_offset = 0;

		/* see what actually matched */
		foreach(lc2, *varinfos)
		{
			ListCell   *lc3;
			int			idx;
			bool		found = false;

			GroupVarInfo *varinfo = (GroupVarInfo *) lfirst(lc2);

			/*
			 * Process a simple Var expression, by matching it to keys
			 * directly. If there's a matching expression, we'll try matching
			 * it later.
			 */
			if (IsA(varinfo->var, Var))
			{
				AttrNumber	attnum = ((Var *) varinfo->var)->varattno;

				/*
				 * Ignore expressions on system attributes. Can't rely on the
				 * bms check for negative values.
				 */
				if (!AttrNumberIsForUserDefinedAttr(attnum))
					continue;

				/* Is the variable covered by the statistics object? */
				if (!bms_is_member(attnum, matched_info->keys))
					continue;

				attnum = attnum + attnum_offset;

				/* ensure sufficient offset */
				Assert(AttrNumberIsForUserDefinedAttr(attnum));

				matched = bms_add_member(matched, attnum);

				found = true;
			}

			/*
			 * XXX Maybe we should allow searching the expressions even if we
			 * found an attribute matching the expression? That would handle
			 * trivial expressions like "(a)" but it seems fairly useless.
			 */
			if (found)
				continue;

			/* expression - see if it's in the statistics object */
			idx = 0;
			foreach(lc3, matched_info->exprs)
			{
				Node	   *expr = (Node *) lfirst(lc3);

				if (equal(varinfo->var, expr))
				{
					AttrNumber	attnum = -(idx + 1);

					attnum = attnum + attnum_offset;

					/* ensure sufficient offset */
					Assert(AttrNumberIsForUserDefinedAttr(attnum));

					matched = bms_add_member(matched, attnum);

					/* there should be just one matching expression */
					break;
				}

				idx++;
			}
		}

		/* Find the specific item that exactly matches the combination */
		for (i = 0; i < stats->nitems; i++)
		{
			int			j;
			MVNDistinctItem *tmpitem = &stats->items[i];

			if (tmpitem->nattributes != bms_num_members(matched))
				continue;

			/* assume it's the right item */
			item = tmpitem;

			/* check that all item attributes/expressions fit the match */
			for (j = 0; j < tmpitem->nattributes; j++)
			{
				AttrNumber	attnum = tmpitem->attributes[j];

				/*
				 * Thanks to how we constructed the matched bitmap above, we
				 * can just offset all attnums the same way.
				 */
				attnum = attnum + attnum_offset;

				if (!bms_is_member(attnum, matched))
				{
					/* nah, it's not this item */
					item = NULL;
					break;
				}
			}

			/*
			 * If the item has all the matched attributes, we know it's the
			 * right one - there can't be a better one. matching more.
			 */
			if (item)
				break;
		}

		/*
		 * Make sure we found an item. There has to be one, because ndistinct
		 * statistics includes all combinations of attributes.
		 */
		if (!item)
			elog(ERROR, "corrupt MVNDistinct entry");

		/* Form the output varinfo list, keeping only unmatched ones */
		foreach(lc, *varinfos)
		{
			GroupVarInfo *varinfo = (GroupVarInfo *) lfirst(lc);
			ListCell   *lc3;
			bool		found = false;

			/*
			 * Let's look at plain variables first, because it's the most
			 * common case and the check is quite cheap. We can simply get the
			 * attnum and check (with an offset) matched bitmap.
			 */
			if (IsA(varinfo->var, Var))
			{
				AttrNumber	attnum = ((Var *) varinfo->var)->varattno;

				/*
				 * If it's a system attribute, we're done. We don't support
				 * extended statistics on system attributes, so it's clearly
				 * not matched. Just keep the expression and continue.
				 */
				if (!AttrNumberIsForUserDefinedAttr(attnum))
				{
					newlist = lappend(newlist, varinfo);
					continue;
				}

				/* apply the same offset as above */
				attnum += attnum_offset;

				/* if it's not matched, keep the varinfo */
				if (!bms_is_member(attnum, matched))
					newlist = lappend(newlist, varinfo);

				/* The rest of the loop deals with complex expressions. */
				continue;
			}

			/*
			 * Process complex expressions, not just simple Vars.
			 *
			 * First, we search for an exact match of an expression. If we
			 * find one, we can just discard the whole GroupVarInfo, with all
			 * the variables we extracted from it.
			 *
			 * Otherwise we inspect the individual vars, and try matching it
			 * to variables in the item.
			 */
			foreach(lc3, matched_info->exprs)
			{
				Node	   *expr = (Node *) lfirst(lc3);

				if (equal(varinfo->var, expr))
				{
					found = true;
					break;
				}
			}

			/* found exact match, skip */
			if (found)
				continue;

			newlist = lappend(newlist, varinfo);
		}

		*varinfos = newlist;
		*ndistinct = item->ndistinct;
		return true;
	}

	return false;
```
## Detailed Description
This function searches through extended statistics objects for the relation to find the most applicable multivariate ndistinct statistic that matches the given variables and expressions. It performs the following steps:

1. **Statistics Object Selection**: Iterates through all available extended statistics objects, filtering for STATS_EXT_NDISTINCT type and matching inheritance settings
2. **Matching Logic**: For each statistics object, counts how many variables and expressions from the input list match the statistics object's keys and expressions
3. **Best Match Selection**: Chooses the statistics object with the highest number of matching expressions, with variables as a tiebreaker
4. **Statistics Loading**: Loads the selected statistics object using statext_ndistinct_load
5. **Item Matching**: Finds the specific MVNDistinctItem within the statistics that exactly matches the combination of variables/expressions
6. **Output Construction**: Updates the ndistinct estimate and removes matched variables from the input varinfos list

The function handles both simple Var nodes and complex expressions, applying appropriate attribute number offsets to handle the internal representation of extended statistics.

## Parameters
- : PlannerInfo structure containing query planning context
- : RelOptInfo for the relation containing the statistics
- : Input/output list of GroupVarInfo structures representing variables/expressions (modified to remove matched items)
- : Output parameter for the estimated number of distinct values

## Dependencies
- Functions called:
  - planner_rt_fetch
  - bms_is_member
  - bms_add_member
  - bms_num_members
  - equal
  - statext_ndistinct_load
  - AttrNumberIsForUserDefinedAttr
- Called from:
  - estimate_num_groups (in selfuncs.c:3635)

## Notes and Other Information
- Returns true if a matching statistics object is found, false otherwise
- Requires at least two matching variables/expressions to consider a statistics object applicable
- Only processes user-defined attributes, ignoring system attributes
- The function includes logic to handle attribute number offsets when expressions are present in the statistics object
- Tie-breaking mechanism should be improved to use object names for stable outcomes
- The function assumes that ndistinct statistics include all combinations of attributes
- Extended statistics must match the inheritance setting of the range table entry