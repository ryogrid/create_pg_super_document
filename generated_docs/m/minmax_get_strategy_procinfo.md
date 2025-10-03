# minmax_get_strategy_procinfo

## Location
[src/backend/access/brin/brin_minmax.c:261-314](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax.c#L261-L314)

## Overview
Caches and returns function manager information for comparison operators used in BRIN minmax operations, providing efficient access to strategy procedures.

## Definition
```c
static FmgrInfo *minmax_get_strategy_procinfo(BrinDesc *bdesc, uint16 attno, Oid subtype, uint16 strategynum)
```

## Detailed Description
This function serves as a caching mechanism for operator procedures used in BRIN minmax operations. It maintains a cache of FmgrInfo structures in the MinmaxOpaque structure to avoid repeated system catalog lookups for the same comparison operators. The function performs lazy initialization of operator procedures and invalidates the cache when the subtype changes.

The function looks up operators in the access method operator family (opfamily) using the AMOPSTRATEGY system catalog. It searches for operators that match the attribute type, subtype, and strategy number combination. Once found, it initializes the function manager information and caches it for subsequent use.

The caching strategy improves performance by avoiding redundant syscache lookups during index operations, which is particularly important for BRIN indexes that may perform many comparisons during summarization and consistency checking.

## Parameters / Member Variables
- `bdesc` (BrinDesc *): BRIN descriptor containing index and operator family information
- `attno` (uint16): Attribute number (1-based) for which to get the procedure
- `subtype` (Oid): Subtype OID for polymorphic operators, or same as main type for non-polymorphic
- `strategynum` (uint16): B-tree strategy number (1-5) identifying the comparison operator

## Dependencies
- Functions called/Symbols referenced:
  - [BrinDesc](../B/BrinDesc.md) (structure type)
  - [MinmaxOpaque](../M/MinmaxOpaque.md) (structure type for opaque data)
  - BTMaxStrategyNumber (constant defining maximum strategy numbers)
  - [SearchSysCache4](../S/SearchSysCache4.md) (function to search system catalogs)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md) (function to extract catalog attributes)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (function to release catalog tuple)
  - [DatumGetObjectId](../D/DatumGetObjectId.md) (function to extract OID from datum)
  - [get_opcode](../g/get_opcode.md) (function to get procedure OID from operator OID)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md) (function to initialize FmgrInfo)
  - InvalidOid (constant representing invalid OID)
  - RegProcedureIsValid (macro to validate procedure OID)

- Called from (representative examples):
  - [brin_minmax_add_value](../b/brin_minmax_add_value.md):99 (for BTLessStrategyNumber comparisons)
  - [brin_minmax_add_value](../b/brin_minmax_add_value.md):113 (for BTGreaterStrategyNumber comparisons)
  - [brin_minmax_consistent](../b/brin_minmax_consistent.md):162, 174, 181, 188 (for various strategy comparisons)
  - [brin_minmax_union](../b/brin_minmax_union.md):226, 239 (for min/max comparisons during union operations)

## Notes and Other Information
- The function is static and only used within the minmax operator class implementation
- Caching is invalidated whenever the subtype changes, ensuring correctness for polymorphic types
- The function mirrors the design of inclusion_get_strategy_procinfo from the inclusion operator class
- Strategy numbers must be between 1 and BTMaxStrategyNumber (typically 5 for B-tree strategies)
- Memory for FmgrInfo structures is allocated in the BRIN descriptor's memory context
- The function will error if a required operator is not found in the operator family
- Cache invalidation is necessary because different subtypes may require different operator implementations

## Simplified Source

```c
static FmgrInfo *
minmax_get_strategy_procinfo(BrinDesc *bdesc, uint16 attno, Oid subtype,
                            uint16 strategynum)
{
    MinmaxOpaque *opaque;

    opaque = (MinmaxOpaque *) bdesc->bd_info[attno - 1]->oi_opaque;

    // Invalidate cache if subtype changed
    if (opaque->cached_subtype != subtype)
    {
        for (uint16 i = 1; i <= BTMaxStrategyNumber; i++)
            opaque->strategy_procinfos[i - 1].fn_oid = InvalidOid;
        opaque->cached_subtype = subtype;
    }

    // Look up procedure if not cached
    if (opaque->strategy_procinfos[strategynum - 1].fn_oid == InvalidOid)
    {
        Form_pg_attribute attr;
        HeapTuple   tuple;
        Oid         opfamily, oprid;

        // Get operator family and attribute info
        opfamily = bdesc->bd_index->rd_opfamily[attno - 1];
        attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);

        // Look up operator in system catalog
        tuple = SearchSysCache4(AMOPSTRATEGY, ObjectIdGetDatum(opfamily),
                               ObjectIdGetDatum(attr->atttypid),
                               ObjectIdGetDatum(subtype),
                               Int16GetDatum(strategynum));

        if (!HeapTupleIsValid(tuple))
            elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
                 strategynum, attr->atttypid, subtype, opfamily);

        // Get procedure OID and initialize function info
        oprid = DatumGetObjectId(SysCacheGetAttrNotNull(AMOPSTRATEGY, tuple,
                                                       Anum_pg_amop_amopopr));
        ReleaseSysCache(tuple);

        fmgr_info_cxt(get_opcode(oprid),
                     &opaque->strategy_procinfos[strategynum - 1],
                     bdesc->bd_context);
    }

    return &opaque->strategy_procinfos[strategynum - 1];
}
```