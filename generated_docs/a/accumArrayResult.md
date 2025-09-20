# accumArrayResult

## Location
[src/backend/utils/adt/arrayfuncs.c:5338-5407](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L5338-L5407)

## Overview
Accumulates one Datum element into an ArrayBuildState structure, handling automatic array growth, memory management, and proper copying of pass-by-reference values.

## Definition

```c
struct_md_array can detoast the array elements later.
	 * However, we must not let construct_md_array modify the ArrayBuildState
	 * because that would mean array_agg_finalfn damages its input, which is
	 * verboten.  Also, this way frequently saves one copying step.)
	 */
	if (!disnull && !astate->typbyval)
	{
		if (astate->typlen == -1)
			dvalue = PointerGetDatum(PG_DETOAST_DATUM_COPY(dvalue));
		else
			dvalue = datumCopy(dvalue, astate->typbyval, astate->typlen);
	}

	astate->dvalues[astate->nelems] = dvalue;
```
## Detailed Description
This function adds a single element to an ArrayBuildState structure, growing the internal arrays as needed. It supports both the older NULL-pointer scheme and the newer initialized-state scheme for array building. When called with a NULL astate (first call in older scheme), it automatically initializes a new ArrayBuildState.

Key operations performed:
1. Initializes ArrayBuildState if astate is NULL (older scheme compatibility)
2. Doubles array capacity when current arrays are full
3. Properly copies pass-by-reference values into the build context
4. Detoasts varlena values to avoid later modification issues
5. Stores the element value and null flag in the arrays

The function ensures that pass-by-reference data is copied into the build context and detoasted if it's a varlena type, preventing issues where the source data might be modified later.

## Parameters / Member Variables
- `astate`: Working state for array building (can be NULL on first call)
- `dvalue`: The Datum value to append to the array
- `disnull`: Whether the new element is NULL
- `element_type`: OID of the element type (must be a valid array element type)
- `rcontext`: Memory context for working state (used only when astate is NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [initArrayResult](../i/initArrayResult.md) (initializes state when astate is NULL)
  - AllocSizeIsValid (validates allocation size limits)
  - [repalloc](../r/repalloc.md) (reallocates arrays when growth is needed)
  - PG_DETOAST_DATUM_COPY (detoasts and copies varlena values)
  - [datumCopy](../d/datumCopy.md) (copies fixed-length pass-by-reference values)
- Called from (representative examples):
  - [array_agg_transfn](array_agg_transfn.md) (array aggregation function)
  - [array_positions](array_positions.md) (finding element positions)
  - [range_agg_transfn](../r/range_agg_transfn.md) (range aggregation)
  - [parse_ident](../p/parse_ident.md) (identifier parsing)
  - [regexp_split_to_array](../r/regexp_split_to_array.md) (regular expression splitting)

## Notes and Other Information
- Supports both older (NULL astate) and newer (initialized astate) usage patterns
- Automatically doubles array capacity when more space is needed
- Enforces maximum array size limits (MaxAllocSize)
- Properly handles both pass-by-value and pass-by-reference data types
- Detoasts varlena values to prevent later modification of the ArrayBuildState
- Always returns a valid ArrayBuildState pointer (never NULL)
- Memory operations are performed in the build state's memory context
- Element type consistency is enforced via assertion when astate is not NULL