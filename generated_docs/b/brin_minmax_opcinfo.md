# brin_minmax_opcinfo

## Location
[src/backend/access/brin/brin_minmax.c:34-63](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax.c#L34-L63)

## Overview
Initializes and returns operator class information for BRIN minmax indexes, setting up the data structure required for storing minimum and maximum values for indexed ranges.

## Definition
```c
Datum brin_minmax_opcinfo(PG_FUNCTION_ARGS)
```

## Detailed Description
This function creates and initializes a BrinOpcInfo structure specifically configured for the minmax operator class. The minmax operator class stores two values (minimum and maximum) for each indexed range, allowing efficient range queries. The function allocates memory for both the BrinOpcInfo structure and an associated MinmaxOpaque structure that contains cached procedure information for comparison operations.

The function sets up the operator class to store exactly 2 values per range (min and max), enables regular null handling, and initializes a type cache for the specified data type. The MinmaxOpaque structure is allocated as part of the same memory block and contains strategy procedure information that will be lazily initialized when needed.

## Parameters / Member Variables
- `typoid` (Oid): The OID of the data type for which the minmax index is being created

## Dependencies
- Functions called/Symbols referenced:
  - [BrinOpcInfo](../B/BrinOpcInfo.md) (structure type)
  - SizeofBrinOpcInfo (macro for calculating structure size)
  - [MinmaxOpaque](../M/MinmaxOpaque.md) (structure type for opaque data)
  - [lookup_type_cache](../l/lookup_type_cache.md) (function to get type cache information)
  - [palloc0](../p/palloc0.md) (memory allocation function)
  - MAXALIGN (memory alignment macro)

- Called from (representative examples):
  - No direct callers found (likely called via function manager)

## Notes and Other Information
- The function uses palloc0 to ensure all memory is zero-initialized, which is important because MinmaxOpaque.strategy_procinfos is initialized lazily
- The oi_nstored field is set to 2 because minmax indexes store both minimum and maximum values
- The oi_regular_nulls flag is set to true, indicating this operator class handles NULLs in the standard way
- Both type cache entries point to the same type cache since min and max values are of the same type
- The MinmaxOpaque structure is aligned and placed immediately after the BrinOpcInfo structure in the same memory allocation

## Simplified Source

```c
Datum
brin_minmax_opcinfo(PG_FUNCTION_ARGS)
{
    Oid typoid = PG_GETARG_OID(0);
    BrinOpcInfo *result;

    // Allocate memory for both BrinOpcInfo and MinmaxOpaque structures
    result = palloc0(MAXALIGN(SizeofBrinOpcInfo(2)) + sizeof(MinmaxOpaque));

    // Configure for minmax: stores 2 values (min and max)
    result->oi_nstored = 2;
    result->oi_regular_nulls = true;

    // Set up opaque data pointer and type cache
    result->oi_opaque = (MinmaxOpaque *)
        MAXALIGN((char *) result + SizeofBrinOpcInfo(2));
    result->oi_typcache[0] = result->oi_typcache[1] =
        lookup_type_cache(typoid, 0);

    PG_RETURN_POINTER(result);
}
```