# minmax_multi_init

## Location
src/backend/access/brin/brin_minmax_multi.c: 486 - 515

## Overview
minmax_multi_init is a constructor function that allocates and initializes a new Ranges structure for BRIN minmax-multi indexes with specified maximum capacity.

## Definition


## Detailed Description
This function serves as the primary constructor for the Ranges data structure used in BRIN minmax-multi access method. It allocates memory for the complete structure including space for the maximum number of Datum values that the ranges can hold. The function uses a single memory allocation to avoid fragmentation and improve performance.

The allocation strategy is designed for efficiency during range operations - by pre-allocating space for the maximum number of values, the function eliminates the need for costly repalloc operations as ranges grow during index operations. The structure is initialized with zero values except for the maxvalues field, which is set to the specified capacity.

The function calculates the total memory needed by combining the fixed header size (using offsetof for the Ranges structure up to the values field) with the variable-length array space needed for the Datum values.

## Parameters / Member Variables
- : Maximum number of Datum values the Ranges structure should be able to hold

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (implicit via memory allocation)
  - offsetof (implicit via structure layout calculation)
- Data structures referenced:
  - [Ranges](../R/Ranges.md)
- Called from (representative examples):
  - [brin_range_deserialize](../b/brin_range_deserialize.md)
  - [brin_minmax_multi_add_value](../b/brin_minmax_multi_add_value.md)

## Notes and Other Information
- Uses palloc0 to ensure the structure is zero-initialized
- Pre-allocates maximum space to avoid repalloc operations during growth
- The function asserts that maxvalues must be positive
- Critical component of BRIN minmax-multi index initialization
- Located in src/backend/access/brin/brin_minmax_multi.c:486-515
- Returns a pointer to the newly allocated and initialized Ranges structure