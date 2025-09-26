# inclusion_get_strategy_procinfo

## Location
[src/backend/access/brin/brin_inclusion.c:608-661](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_inclusion.c#L608-L661)

## Overview
A static helper function that caches and returns procedure information for given strategy numbers in BRIN inclusion opclasses, optimizing repeated access to operator procedures through syscache caching.

## Definition

```c
struct, to
	 * avoid repetitive syscache lookups.  If the sub-type is changed,
	 * invalidate all the cached entries.
	 */
	if (opaque->cached_subtype != subtype)
	{
		uint16		i;

		for (i = 1; i <= RTMaxStrategyNumber; i++)
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
This function serves as a procedure lookup and caching mechanism for BRIN inclusion opclasses. It retrieves the procedure corresponding to a given sub-type and strategy number, where the index's data type acts as the left-hand side of the operator and the provided sub-type as the right-hand side. The function implements an intelligent caching strategy to avoid repetitive syscache lookups by storing procedures for the last accessed sub-type in the opaque structure.

The function enforces strict type matching between the opclass data type and column/expression data type, throwing errors when implicit casting would be required. This design choice encourages proper configuration where the storage data type matches the opclass data type.

The caching mechanism invalidates all stored procedures when the sub-type changes, ensuring consistency while maximizing performance for repeated access patterns with the same sub-type.

## Parameters
- `bdesc`: BRIN descriptor containing index metadata and opclass information
- `attno`: Attribute number (1-based) identifying the specific column in the index
- `subtype`: OID of the sub-type to be used as the right-hand side of the operator
- `strategynum`: Strategy number (1 to RTMaxStrategyNumber) identifying the specific operator within the opclass

## Dependencies
- Functions called/Symbols referenced:
  - [BrinDesc](../B/BrinDesc.md)
  - [InclusionOpaque](../I/InclusionOpaque.md)
  - RTMaxStrategyNumber
  - TupleDescAttr
  - [SearchSysCache4](../S/SearchSysCache4.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [Int16GetDatum](../I/Int16GetDatum.md)
  - HeapTupleIsValid
  - [DatumGetObjectId](../D/DatumGetObjectId.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - RegProcedureIsValid
  - [get_opcode](../g/get_opcode.md)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)

- Called from (representative examples):
  - [brin_inclusion_consistent](../b/brin_inclusion_consistent.md) (multiple calls for different strategy operations)

## Notes and Other Information
- This function mirrors the design of  and changes should be synchronized between both implementations
- The caching strategy is optimized for scenarios where multiple operations use the same sub-type consecutively
- Type casting is intentionally not supported to encourage proper opclass configuration
- The function assumes proper opclass configuration and will throw errors for missing pg_amop entries
- Strategy numbers must be within the valid range (1 to RTMaxStrategyNumber)
- The function operates within the BRIN descriptor's memory context for procedure information storage