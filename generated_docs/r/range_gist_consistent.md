# range_gist_consistent

## Location
[src/backend/utils/adt/rangetypes_gist.c:191-244](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L191-L244)

## Overview
Implements the GiST consistency check operation for range types, determining whether a query matches an index entry during index searches.

## Definition

```c
Datum
range_gist_consistent(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is the core consistency checking function for GiST indexes on range types. It determines whether a given query (which can be a range, multirange, or element) matches an index entry based on the specified search strategy. This function serves as the entry point that dispatches to specialized consistency checking functions based on whether the index entry is a leaf or internal node, and what type of query is being performed.

The function handles three types of queries:
1. Range queries (subtype is invalid or ANYRANGEOID)
2. Multirange queries (subtype is ANYMULTIRANGEOID)  
3. Element queries (any other subtype)

It also distinguishes between leaf nodes (actual data) and internal nodes (bounding ranges) in the GiST tree structure.

## Parameters / Member Variables
- : GiST index entry containing the key (range) to check against
- : The search query value (range, multirange, or element)
- : Strategy number indicating the type of search operation (overlaps, contains, etc.)
- : OID indicating the query type (range, multirange, or element)
- : Output parameter set to false since all operations are exact

## Dependencies
- Functions called/Symbols referenced:
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
- Called from (representative examples):
  - GiST index access method during query execution

## Notes and Other Information
- This function sets recheck to false because all range operations are exact and don't require rechecking at the heap level
- The function acts as a dispatcher, routing to appropriate specialized consistency functions based on node type and query type
- Part of the GiST operator class implementation for range types
- Located in src/backend/utils/adt/rangetypes_gist.c:191-244

## Simplified Source

```c
Datum
range_gist_consistent(PG_FUNCTION_ARGS)
{
	GISTENTRY  *entry = (GISTENTRY *) PG_GETARG_POINTER(0);
	Datum		query = PG_GETARG_DATUM(1);
	StrategyNumber strategy = (StrategyNumber) PG_GETARG_UINT16(2);
	Oid			subtype = PG_GETARG_OID(3);
	bool	   *recheck = (bool *) PG_GETARG_POINTER(4);
	RangeType  *key = DatumGetRangeTypeP(entry->key);
	TypeCacheEntry *typcache;
	bool		result;

	// All range operations are exact
	*recheck = false;

	typcache = range_get_typcache(fcinfo, RangeTypeGetOid(key));

	// Dispatch based on node type (leaf vs internal) and query type
	if (GIST_LEAF(entry)) {
		// Leaf node: check against actual data
		if (!OidIsValid(subtype) || subtype == ANYRANGEOID)
			result = range_gist_consistent_leaf_range(typcache, strategy, key,
													  DatumGetRangeTypeP(query));
		else if (subtype == ANYMULTIRANGEOID)
			result = range_gist_consistent_leaf_multirange(typcache, strategy, key,
														   DatumGetMultirangeTypeP(query));
		else
			result = range_gist_consistent_leaf_element(typcache, strategy, key, query);
	} else {
		// Internal node: check against bounding range
		if (!OidIsValid(subtype) || subtype == ANYRANGEOID)
			result = range_gist_consistent_int_range(typcache, strategy, key,
													 DatumGetRangeTypeP(query));
		else if (subtype == ANYMULTIRANGEOID)
			result = range_gist_consistent_int_multirange(typcache, strategy, key,
														  DatumGetMultirangeTypeP(query));
		else
			result = range_gist_consistent_int_element(typcache, strategy, key, query);
	}

	PG_RETURN_BOOL(result);
}
```