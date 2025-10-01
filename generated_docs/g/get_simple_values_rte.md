# get_simple_values_rte

## Location
[src/backend/utils/adt/ruleutils.c:5835-5903](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L5835-L5903)

## Overview
Detects whether a query looks like SELECT ... FROM VALUES() with no need to rename the output columns of the VALUES RTE, and returns the VALUES RTE if found.

## Definition

```c
structure.  However, DefineView might have modified the tlist by
	 * injecting new column aliases, or we might have some other column
	 * aliases forced by a resultDesc.  We can only simplify if the RTE's
	 * column names match the names that get_target_list() would select.
	 */
	if (result)
	{
		ListCell   *lcn;
		int			colno;

		if (list_length(query->targetList) != list_length(result->eref->colnames))
			return NULL;		/* this probably cannot happen */
		colno = 0;
		forboth(lc, query->targetList, lcn, result->eref->colnames)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			char	   *cname = strVal(lfirst(lcn));
			char	   *colname;

			if (tle->resjunk)
				return NULL;	/* this probably cannot happen */

			/* compute name that get_target_list would use for column */
			colno++;
			if (resultDesc && colno <= resultDesc->natts)
				colname = NameStr(TupleDescAttr(resultDesc, colno - 1)->attname);
			else
				colname = tle->resname;

			/* does it match the VALUES RTE? */
			if (colname == NULL || strcmp(colname, cname) != 0)
				return NULL;	/* column name has been changed */
		}
	}

	return result;
```
## Detailed Description
This function analyzes a query to determine if it has the simple form "SELECT ... FROM VALUES()" without column renaming requirements. It scans the query's range table (rtable) to find exactly one VALUES RTE that is marked as inFromCl (in the FROM clause). The function also validates that the column names in the target list match what get_target_list() would select, ensuring no column aliases have been introduced that would complicate the representation.

The function is designed to work even when the query contains OLD or NEW rule RTEs, focusing only on finding a single VALUES RTE in the FROM clause. If multiple VALUES RTEs exist or if column names don't match expected values, the function returns NULL to indicate the query is not in the simple form.

## Parameters / Member Variables
- `query`: The Query structure to analyze for simple VALUES pattern
- `resultDesc`: Optional tuple descriptor that may provide column name constraints

## Dependencies
- Functions called/Symbols referenced:
  - RTE_VALUES (range table entry type constant)
  - RTE_RELATION (range table entry type constant)  
  - forboth (macro for iterating over two lists simultaneously)
- Called from (representative examples):
  - [get_basic_select_query](get_basic_select_query.md) (src/backend/utils/adt/ruleutils.c:5922)

## Notes and Other Information
- Only works with auto-generated sub-queries with restricted structure from the parser
- [DefineView](../D/DefineView.md) might modify the target list by injecting column aliases, which this function detects
- Returns NULL if the query structure is too complex or if column names have been modified
- Part of the PostgreSQL rule utilities for query deparsing and rule generation

## Simplified Source

```c
static RangeTblEntry *get_simple_values_rte(Query *query, TupleDesc resultDesc)
{
    RangeTblEntry *result = NULL;

    // Scan rtable to find exactly one VALUES RTE in FROM clause
    foreach(lc, query->rtable)
    {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

        if (rte->rtekind == RTE_VALUES && rte->inFromCl)
        {
            if (result)
                return NULL;  // Multiple VALUES RTEs not allowed
            result = rte;
        }
        else if (rte->rtekind == RTE_RELATION && !rte->inFromCl)
            continue;  // Ignore OLD/NEW rule entries
        else
            return NULL;  // Other RTE types make this non-simple
    }

    // Verify column names match expected values (no renaming)
    if (result)
    {
        if (list_length(query->targetList) != list_length(result->eref->colnames))
            return NULL;

        int colno = 0;
        forboth(lc, query->targetList, lcn, result->eref->colnames)
        {
            TargetEntry *tle = (TargetEntry *) lfirst(lc);
            char *cname = strVal(lfirst(lcn));
            char *colname;

            if (tle->resjunk)
                return NULL;

            // Get expected column name
            colno++;
            if (resultDesc && colno <= resultDesc->natts)
                colname = NameStr(TupleDescAttr(resultDesc, colno - 1)->attname);
            else
                colname = tle->resname;

            // Check if names match
            if (colname == NULL || strcmp(colname, cname) != 0)
                return NULL;  // Column name changed
        }
    }

    return result;
}
```