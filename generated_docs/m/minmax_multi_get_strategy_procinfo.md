# minmax_multi_get_strategy_procinfo

## Location
[src/backend/access/brin/brin_minmax_multi.c:2899-2953](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L2899-L2953)

## Overview
Cache and return comparison operators for specific data types and strategy numbers in the minmax-multi operator class.

## Definition

```c
struct,
	 * to avoid repetitive syscache lookups.  If the subtype changed,
	 * invalidate all the cached entries.
	 */
	if (opaque->cached_subtype != subtype)
	{
		uint16		i;

		for (i = 1; i <= BTMaxStrategyNumber; i++)
			opaque->strategy_procinfos[i - 1].fn_oid = InvalidOid;
		opaque->cached_subtype = subtype;
	}

	if (opaque->strategy_procinfos[strategynum - 1].fn_oid == InvalidOid)
	{
		Form_pg_attribute attr;
		HeapTuple	tuple;
		Oid			opfamily,
					oprid;

		opfamily = bdesc->bd_index->rd_opfamily[attno - 1];
		attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);
		tuple = SearchSysCache4(AMOPSTRATEGY, ObjectIdGetDatum(opfamily),
								ObjectIdGetDatum(attr->atttypid),
								ObjectIdGetDatum(subtype),
								Int16GetDatum(strategynum));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
				 strategynum, attr->atttypid, subtype, opfamily);

		oprid = DatumGetObjectId(SysCacheGetAttrNotNull(AMOPSTRATEGY, tuple,
														Anum_pg_amop_amopopr));
		ReleaseSysCache(tuple);
		Assert(RegProcedureIsValid(oprid));

		fmgr_info_cxt(get_opcode(oprid),
					  &opaque->strategy_procinfos[strategynum - 1],
					  bdesc->bd_context);
	}

	return &opaque->strategy_procinfos[strategynum - 1];
```
## Detailed Description
This static function provides cached access to strategy operators (comparison functions) for the minmax-multi operator class. Unlike minmax_multi_get_procinfo which caches support functions, this function caches operators based on strategy numbers (like BTLessStrategyNumber) for specific data types.

The function implements a sophisticated caching mechanism that invalidates cached entries when the subtype changes, ensuring correct operator resolution for different data types. It performs system catalog lookups to find the appropriate operator in the operator family and caches the resulting function manager info.

The caching uses the MinmaxMultiOpaque structure's strategy_procinfos array and tracks the cached_subtype to detect when cache invalidation is needed.

## Parameters / Member Variables
- : BrinDesc pointer - BRIN index descriptor containing metadata
- : uint16 - Attribute number (1-based) for the column
- : Oid - Object identifier of the data type for operator resolution
- : uint16 - Strategy number (1 to BTMaxStrategyNumber)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache4](../S/SearchSysCache4.md) (AMOPSTRATEGY lookup)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - [DatumGetObjectId](../D/DatumGetObjectId.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - RegProcedureIsValid
  - [get_opcode](../g/get_opcode.md)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)
- Called from (representative examples):
  - [AssertCheckExpandedRanges](../A/AssertCheckExpandedRanges.md)
  - [has_matching_range](../h/has_matching_range.md)
  - [range_contains_value](../r/range_contains_value.md)
  - [ensure_free_space_in_buffer](../e/ensure_free_space_in_buffer.md)
  - [range_add_value](../r/range_add_value.md)
  - [compactify_ranges](../c/compactify_ranges.md)
  - [brin_minmax_multi_add_value](../b/brin_minmax_multi_add_value.md)
  - [brin_minmax_multi_consistent](../b/brin_minmax_multi_consistent.md)
  - [brin_minmax_multi_union](../b/brin_minmax_multi_union.md)

## Notes and Other Information
- This is a static helper function used internally within the minmax-multi implementation
- Invalidates all cached strategy operators when the subtype changes to ensure type safety
- Performs system catalog lookups using the AMOPSTRATEGY cache for operator family resolution
- Strategy numbers must be between 1 and BTMaxStrategyNumber
- Extensively used throughout the minmax-multi implementation for comparison operations
- The cached operators are stored in the BRIN index's memory context for proper lifecycle management