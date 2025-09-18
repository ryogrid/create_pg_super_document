# test_indoption

## Location
src/backend/utils/adt/amutils.c: 117 - 150

## Overview
Common utility function that tests specific bits in the indoptions array of a pg_index tuple to determine boolean properties of index columns.

## Definition
```c
static bool test_indoption(HeapTuple tuple, int attno, bool guard, int16 iopt_mask, int16 iopt_expect, bool *res)
```

## Detailed Description
This function provides a standardized way to test index column properties that are encoded as bits in the indoptions array of pg_index catalog entries. It extracts the indoptions value for a specific column and performs bitwise testing against a provided mask and expected value. The function includes a guard parameter for early termination and uses the system cache to efficiently retrieve the indoptions data. It returns a status indicating whether a definitive boolean result was determined, with the actual result stored in the res parameter.

## Parameters / Member Variables
- `tuple`: The pg_index heap tuple containing the index metadata
- `attno`: The 1-based attribute number identifying which index column to test
- `guard`: If false, forces a boolean false result without testing (optimization for callers)
- `iopt_mask`: Bitmask specifying which indoption bits are relevant for this test
- `iopt_expect`: Expected bit pattern for a "true" result (should be 0 or iopt_mask)
- `res`: Output parameter to store the boolean test result

## Dependencies
- Functions called/Symbols referenced:
  - SysCacheGetAttrNotNull (retrieve indoptions from system cache)
  - int2vector (data type for indoptions array)
  - DatumGetPointer (convert Datum to pointer)
- Called from (representative examples):
  - indexam_property (multiple calls for different property tests)

## Notes and Other Information
- The function is static and only used within amutils.c
- Returns false to indicate a NULL result when the property is unknown or inapplicable
- Returns true when a definitive boolean result is available in *res
- The indoptions array uses 0-based indexing internally, so attno is decremented by 1
- Used for testing properties like ASC/DESC ordering, NULLS FIRST/LAST positioning
- The guard parameter allows callers to short-circuit testing when conditions make the result predictable