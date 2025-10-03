# range_contains_value

## Location
[src/backend/access/brin/brin_minmax_multi.c:1045-1133](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L1045-L1133)

## Overview
Determines whether a new value is already contained within a BRIN minmax_multi range list by checking intervals and exact values.

## Definition
static bool range_contains_value(BrinDesc *bdesc, Oid colloid, AttrNumber attno, Form_pg_attribute attr, Ranges *ranges, Datum newval, bool full)

## Detailed Description
This function implements an efficient containment check for values within BRIN minmax_multi range structures. It employs a two-phase search strategy:

1. **Range Interval Check**: First checks if the value falls within any of the stored intervals using , which optimizes by checking against the overall min/max bounds before examining individual intervals.

2. **Exact Value Search**: If no range contains the value, searches through stored exact values using either binary search (for 16+ sorted values) or linear search (for fewer values).

The function can operate in two modes controlled by the  parameter:
- : Only searches sorted values, potentially allowing false negatives in the unsorted portion (used during range building)
- : Searches all values including unsorted ones (used for complete containment checks)

## Parameters / Member Variables
- : BRIN descriptor containing metadata and function information
- : Collation OID for comparison operations
- : Attribute number within the BRIN index
- : Form_pg_attribute structure containing attribute metadata including type OID
- : Ranges structure containing the interval and value data
- : The Datum value to search for
- : Boolean flag determining whether to search unsorted values

## Dependencies
- Functions called/Symbols referenced:
  - [has_matching_range](../h/has_matching_range.md)
  - [minmax_multi_get_strategy_procinfo](../m/minmax_multi_get_strategy_procinfo.md)
  - [bsearch_arg](../b/bsearch_arg.md)
  - [compare_values](../c/compare_values.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [BrinDesc](../B/BrinDesc.md)
  - [Ranges](../R/Ranges.md)
  - [compare_context](../c/compare_context.md)

- Called from (representative examples):
  - [range_add_value](range_add_value.md)

## Notes and Other Information
- Uses a threshold of 16 sorted values to switch between linear and binary search
- The binary search optimization significantly improves performance for large value sets
- False negatives are acceptable during range building (full=false) as deduplication occurs before serialization
- Serialized ranges never contain unsorted values, ensuring no false negatives during querying
- The function is static and only used within the BRIN minmax_multi implementation

## Simplified Source

```c
static bool
range_contains_value(BrinDesc *bdesc, Oid colloid,
                     AttrNumber attno, Form_pg_attribute attr,
                     Ranges *ranges, Datum newval, bool full)
{
    Oid typid = attr->atttypid;

    // First check if value falls within any existing ranges
    if (has_matching_range(bdesc, colloid, ranges, newval, attno, typid))
        return true;

    // Get equality comparison function
    FmgrInfo *cmpEqualFn = minmax_multi_get_strategy_procinfo(bdesc, attno, typid,
                                                              BTEqualStrategyNumber);

    // Search sorted values - use binary search for 16+ values, linear for fewer
    if (ranges->nsorted >= 16) {
        compare_context cxt;
        cxt.colloid = ranges->colloid;
        cxt.cmpFn = ranges->cmp;

        if (bsearch_arg(&newval, &ranges->values[2 * ranges->nranges],
                        ranges->nsorted, sizeof(Datum),
                        compare_values, (void *) &cxt) != NULL)
            return true;
    }
    else {
        // Linear search through sorted values
        for (int i = 2 * ranges->nranges;
             i < 2 * ranges->nranges + ranges->nsorted; i++) {
            Datum compar = FunctionCall2Coll(cmpEqualFn, colloid,
                                           newval, ranges->values[i]);
            if (DatumGetBool(compar))
                return true;
        }
    }

    // If not searching unsorted values, we're done
    if (!full)
        return false;

    // Search unsorted values (linear search only)
    for (int i = 2 * ranges->nranges + ranges->nsorted;
         i < 2 * ranges->nranges + ranges->nvalues; i++) {
        Datum compar = FunctionCall2Coll(cmpEqualFn, colloid,
                                       newval, ranges->values[i]);
        if (DatumGetBool(compar))
            return true;
    }

    return false;
}
```