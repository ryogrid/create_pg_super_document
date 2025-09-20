# test_huge_distances

## Location
[src/test/modules/test_integerset/test_integerset.c:519-623](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_integerset/test_integerset.c#L519-L623)

## Overview
Tests IntegerSet functionality with values that have distances greater than 2^60 between them, specifically testing the Simple-8b encoding limitations.

## Definition

```c
structure.
	 */
	while (num_values < 1000)
	{
		val += pg_prng_uint32(&pg_global_prng_state);
		values[num_values++] = val;
	}

	/* Create an IntegerSet using these values */
	intset = intset_create();
```
## Detailed Description
This function tests the IntegerSet implementation with integers that are more than 2^60 apart. The Simple-8b encoding used by the set implementation can only encode values up to 2^60, making large differences particularly important to test. 

The test creates a sequence of values with carefully calculated distances:
1. Starts with value 0
2. Adds increments of (2^60 - 1), (2^60), and (2^60 + 1) and (2^60 + 2) to test boundary conditions
3. Fills remaining slots with random increments to ensure tree structure flushing
4. Validates both membership queries and iterator functionality across all test values

The function ensures that the IntegerSet can handle extreme value ranges and that the internal encoding properly manages large gaps between stored integers.

## Parameters / Member Variables
- None (void function)

## Dependencies  
- Functions called/Symbols referenced:
  -  (with NOTICE and ERROR levels)
  - 
  - 
  - 
  - 
  - 
  - 
  -  (type)
  -  (format specifier)
  -  (global variable)

- Called from:
  -  (src/test/modules/test_integerset/test_integerset.c:101)
  -  (src/test/modules/test_integerset/test_integerset.c:111)

## Notes and Other Information
- Static function used exclusively for testing IntegerSet functionality with extreme value ranges
- Tests specifically target the 2^60 boundary limitation of Simple-8b encoding
- Uses UINT64CONST macro to define large constant values (1152921504606846976 = 2^60)
- Creates up to 1000 test values, with initial values having huge gaps and later values being more densely packed
- Performs comprehensive validation including boundary value membership testing and complete iteration verification
- Critical for ensuring IntegerSet robustness with sparse, widely-distributed integer sets
- Part of the test_integerset module's comprehensive test suite