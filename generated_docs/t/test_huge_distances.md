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

## Parameters
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

## Simplified Source

```c
static void
test_huge_distances(void)
{
    IntegerSet *intset;
    uint64 values[1000];
    int num_values = 0;
    uint64 val = 0;

    elog(NOTICE, "testing intset with distances > 2^60 between values");

    // Start with 0
    values[num_values++] = val;

    // Test differences around the 2^60 boundary (Simple-8b encoding limit)
    val += UINT64CONST(1152921504606846976) - 1;  // 2^60 - 1
    values[num_values++] = val;
    val += UINT64CONST(1152921504606846976) - 1;  // 2^60 - 1
    values[num_values++] = val;
    val += UINT64CONST(1152921504606846976);      // 2^60
    values[num_values++] = val;
    val += UINT64CONST(1152921504606846976);      // 2^60
    values[num_values++] = val;
    val += UINT64CONST(1152921504606846976);      // 2^60
    values[num_values++] = val;
    val += UINT64CONST(1152921504606846976) + 1;  // 2^60 + 1
    values[num_values++] = val;
    val += UINT64CONST(1152921504606846976) + 1;  // 2^60 + 1
    values[num_values++] = val;
    val += UINT64CONST(1152921504606846976) + 1;  // 2^60 + 1
    values[num_values++] = val;
    val += UINT64CONST(1152921504606846976) + 2;  // 2^60 + 2
    values[num_values++] = val;
    val += UINT64CONST(1152921504606846976) + 2;  // 2^60 + 2
    values[num_values++] = val;
    val += UINT64CONST(1152921504606846976);      // 2^60
    values[num_values++] = val;

    // Add more smaller values to force tree structure packing
    while (num_values < 1000) {
        val += pg_prng_uint32(&pg_global_prng_state);
        values[num_values++] = val;
    }

    // Create IntegerSet and add all values
    intset = intset_create();
    for (int i = 0; i < num_values; i++) {
        intset_add_member(intset, values[i]);
    }

    // Test membership around each value
    for (int i = 0; i < num_values; i++) {
        uint64 y = values[i];

        // Test y-1 (should only be member if consecutive with previous)
        if (y > 0) {
            bool expected = (i > 0 && values[i-1] == y - 1);
            if (intset_is_member(intset, y - 1) != expected) {
                elog(ERROR, "intset_is_member failed for %llu", y - 1);
            }
        }

        // Test y (should always be member)
        if (!intset_is_member(intset, y)) {
            elog(ERROR, "intset_is_member failed for %llu", y);
        }

        // Test y+1 (should only be member if consecutive with next)
        bool expected = (i < num_values - 1 && values[i + 1] == y + 1);
        if (intset_is_member(intset, y + 1) != expected) {
            elog(ERROR, "intset_is_member failed for %llu", y + 1);
        }
    }

    // Test iteration returns all values in order
    intset_begin_iterate(intset);
    for (int i = 0; i < num_values; i++) {
        uint64 x;
        if (!intset_iterate_next(intset, &x) || x != values[i]) {
            elog(ERROR, "intset_iterate_next failed for %llu", values[i]);
        }
    }

    // Verify no extra values
    uint64 x;
    if (intset_iterate_next(intset, &x)) {
        elog(ERROR, "unexpected extra value in iteration: %llu", x);
    }
}
```