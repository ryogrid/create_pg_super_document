# get_stats_slot_range

## Location
[src/backend/utils/adt/selfuncs.c:6090-6152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L6090-L6152)

## Overview
Scans a statistics slot to find minimum and maximum values among the stored values, used as a subroutine for get_variable_range to update min/max/have_data according to statistics array contents.

## Definition

```c
static void
get_stats_slot_range(AttStatsSlot *sslot, Oid opfuncoid, FmgrInfo *opproc,
					 Oid collation, int16 typLen, bool typByVal,
					 Datum *min, Datum *max, bool *p_have_data)
```
## Detailed Description
This function examines all values in a statistics slot (AttStatsSlot) to determine the minimum and maximum values using a specified comparison operator. It iterates through the slot's values array, comparing each value against the current min/max using the provided comparison function. The function handles the initial case when no data has been processed yet by setting both min and max to the first encountered value. When new extreme values are found, they are copied using datumCopy to ensure proper memory management.

## Parameters / Member Variables
- `*sslot`: AttStatsSlot containing the statistics values to scan
- `opfuncoid`: OID of the comparison function to use for ordering values
- `*opproc`: FmgrInfo structure for the comparison function (cached for efficiency)
- `collation`: Collation to use when calling the comparison function
- `typLen`: Length of the data type (-1 for variable length types)
- `typByVal`: Whether the data type is passed by value or reference
- `*min`: Pointer to current minimum value, updated if a smaller value is found
- `*max`: Pointer to current maximum value, updated if a larger value is found
- `*p_have_data`: Pointer to boolean indicating whether any data has been processed
## Dependencies
- Functions called/Symbols referenced:
  - [AttStatsSlot](../A/AttStatsSlot.md)
  - [fmgr_info](../f/fmgr_info.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [datumCopy](../d/datumCopy.md)
- Called from (representative examples):
  - [get_variable_range](get_variable_range.md)

## Notes and Other Information
This is a static helper function specifically designed for range estimation in PostgreSQL's query planner. It uses the function manager (fmgr) system to call comparison operators dynamically, allowing it to work with any orderable data type. The function optimizes by caching the comparison function in the FmgrInfo structure to avoid repeated lookups. Memory management is handled carefully by copying found extreme values to prevent issues with temporary or freed memory.

## Simplified Source

```c
static void
get_stats_slot_range(AttStatsSlot *sslot, Oid opfuncoid, FmgrInfo *opproc,
                     Oid collation, int16 typLen, bool typByVal,
                     Datum *min, Datum *max, bool *p_have_data)
{
    Datum current_min = *min;
    Datum current_max = *max;
    bool have_data = *p_have_data;
    bool updated_min = false;
    bool updated_max = false;

    // Cache the comparison function if not already done
    if (opproc->fn_oid != opfuncoid)
        fmgr_info(opfuncoid, opproc);

    // Scan all values in the statistics slot
    for (int i = 0; i < sslot->nvalues; i++)
    {
        // Initialize min/max with first value if no data yet
        if (!have_data)
        {
            current_min = current_max = sslot->values[i];
            updated_min = updated_max = true;
            *p_have_data = have_data = true;
            continue;
        }

        // Check if current value is smaller than min
        if (DatumGetBool(FunctionCall2Coll(opproc, collation,
                                           sslot->values[i], current_min)))
        {
            current_min = sslot->values[i];
            updated_min = true;
        }

        // Check if current value is larger than max
        if (DatumGetBool(FunctionCall2Coll(opproc, collation,
                                           current_max, sslot->values[i])))
        {
            current_max = sslot->values[i];
            updated_max = true;
        }
    }

    // Copy new extreme values if found
    if (updated_min)
        *min = datumCopy(current_min, typByVal, typLen);
    if (updated_max)
        *max = datumCopy(current_max, typByVal, typLen);
}
```