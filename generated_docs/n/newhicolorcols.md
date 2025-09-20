# newhicolorcols

## Location
[src/backend/regex/regc_color.c:469-521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_color.c#L469-L521)

## Overview
Extends the hicolormap 2D array horizontally by duplicating existing columns, effectively doubling the width of the color mapping table.

## Definition

```c
struct colormap *cm)
{
	color	   *newarray;
	int			r,
				c;

	if (cm->hiarraycols >= INT_MAX / (cm->maxarrayrows * 2))
	{
		CERR(REG_ESPACE);
		return;
	}
	newarray = (color *) REALLOC(cm->hicolormap,
								 cm->maxarrayrows *
								 cm->hiarraycols * 2 * sizeof(color));
	if (newarray == NULL)
	{
		CERR(REG_ESPACE);
		return;
	}
	cm->hicolormap = newarray;

	/* Duplicate existing columns to the right, and increase ref counts */
	/* Must work backwards in the array because we realloc'd in place */
	for (r = cm->hiarrayrows - 1; r >= 0; r--)
	{
		color	   *oldrowptr = &newarray[r * cm->hiarraycols];
		color	   *newrowptr = &newarray[r * cm->hiarraycols * 2];
		color	   *newrowptr2 = newrowptr + cm->hiarraycols;

		for (c = 0; c < cm->hiarraycols; c++)
		{
			color		co = oldrowptr[c];

			newrowptr[c] = newrowptr2[c] = co;
			cm->cd[co].nuchrs++;
		}
	}

	cm->hiarraycols *= 2;
}

/*
 * subcolorcvec - allocate new subcolors to cvec members, fill in arcs
 *
 * For each chr "c" represented by the cvec, do the equivalent of
 * newarc(v->nfa, PLAIN, subcolor(v->cm, c), lp, rp);
```
## Detailed Description
The  function is responsible for expanding the hicolormap array horizontally. It creates a new set of columns by copying the existing columns to the right, essentially doubling the width of the 2D color mapping array. The function performs in-place reallocation and works backwards through the rows to avoid overwriting data during the duplication process. After copying, it updates the reference counts for all colors to maintain proper bookkeeping.

## Parameters
- `cm`: Pointer to the colormap structure containing the hicolormap array and related metadata

## Dependencies
- Functions called/Symbols referenced:
  - CERR (error reporting macro)
  - REALLOC (memory reallocation macro)
  - REG_ESPACE (error code constant)
- Called from (representative examples):
  - [subcolorcvec](../s/subcolorcvec.md) (at line 587)

## Notes and Other Information
- Does not return a value (void function)
- Doubles the  value after successful expansion
- Includes overflow protection by checking against INT_MAX before allocation
- Uses backwards iteration through rows to safely duplicate data in-place after reallocation
- Increases color reference counts (nuchrs) for all duplicated color entries
- Part of the regex engine's color compression system that manages efficient color-to-character mappings