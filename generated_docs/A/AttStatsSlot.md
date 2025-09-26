# AttStatsSlot

## Location
src/include/utils/lsyscache.h: 46 - 62

## Overview
AttStatsSlot is a result struct used to extract and manage statistical information from PostgreSQL's pg_statistic system catalog, encapsulating both metadata and data arrays for query optimization.

## Definition
```c
typedef struct AttStatsSlot
{
    /* Always filled: */
    Oid         staop;          /* Actual staop for the found slot */
    Oid         stacoll;        /* Actual collation for the found slot */
    /* Filled if ATTSTATSSLOT_VALUES is specified: */
    Oid         valuetype;      /* Actual datatype of the values */
    Datum      *values;         /* slot's "values" array, or NULL if none */
    int         nvalues;        /* length of values[], or 0 */
    /* Filled if ATTSTATSSLOT_NUMBERS is specified: */
    float4     *numbers;        /* slot's "numbers" array, or NULL if none */
    int         nnumbers;       /* length of numbers[], or 0 */

    /* Remaining fields are private to get_attstatsslot/free_attstatsslot */
    void       *values_arr;     /* palloc'd values array, if any */
    void       *numbers_arr;    /* palloc'd numbers array, if any */
} AttStatsSlot;
```

## Detailed Description
AttStatsSlot serves as a structured container for extracting statistical data from PostgreSQL's pg_statistic system catalog. This struct is the primary interface between the statistics storage system and the query optimizer, providing a clean abstraction for accessing column statistics that drive cost-based optimization decisions.

The struct is designed to handle the dual nature of PostgreSQL statistics: both discrete values (like most common values) and numeric data (like histogram bounds or frequencies). The memory management is carefully designed to ensure that all extracted data remains valid until explicitly freed, with private fields tracking the underlying palloc'd memory regions.

This struct supports PostgreSQL's flexible statistics system where different types of statistics (stakind) can be stored with different operators (staop) and collations (stacoll), allowing the optimizer to find the most appropriate statistical information for a given query context.

## Parameters / Member Variables
- `staop`: The OID of the statistical operator actually found in the statistics slot
- `stacoll`: The OID of the collation associated with the statistics slot
- `valuetype`: The OID of the actual datatype of elements in the values array (filled when ATTSTATSSLOT_VALUES flag is used)
- `values`: Pointer to an array of Datum values representing statistical data like most common values (NULL if not requested or unavailable)
- `nvalues`: Number of elements in the values array, or 0 if no values
- `numbers`: Pointer to an array of float4 numbers representing statistical data like frequencies or histogram bounds (NULL if not requested or unavailable)
- `nnumbers`: Number of elements in the numbers array, or 0 if no numbers
- `values_arr`: Private field pointing to the palloc'd array object containing values data
- `numbers_arr`: Private field pointing to the palloc'd array object containing numbers data

## Dependencies
- Functions called/Symbols referenced:
  - float4 (PostgreSQL single-precision float type)
  - Oid (PostgreSQL object identifier type)
  - Datum (PostgreSQL generic data value type)
- Called from (representative examples):
  - get_attstatsslot (primary constructor function)
  - free_attstatsslot (memory cleanup function)
  - ExecHashBuildSkewHash
  - var_eq_const
  - histogram_selectivity
  - eqjoinsel

## Notes and Other Information
- The struct must be initialized using `get_attstatsslot` and cleaned up with `free_attstatsslot` to prevent memory leaks
- The private `values_arr` and `numbers_arr` fields should never be accessed directly by external code
- The `values` array elements are of type `valuetype`, which varies depending on the column's data type
- The `numbers` array is always float4, regardless of the original statistic type
- Supports flags ATTSTATSSLOT_VALUES and ATTSTATSSLOT_NUMBERS to selectively extract different types of statistical data
- Used extensively throughout PostgreSQL's selectivity estimation functions for cost-based query optimization
- Memory layout is designed to support efficient access patterns in the query planner
- Safe to memset to zero for initialization if `get_attstatsslot` might not be called