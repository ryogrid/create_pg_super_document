# test_pattern

## Location
[src/test/modules/test_integerset/test_integerset.c:135-320](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_integerset/test_integerset.c#L135-L320)

## Overview
Comprehensive test function that validates IntegerSet functionality using repeating bit patterns, performing thorough testing of insertion, lookup, iteration, and memory usage operations.

## Definition

```c
static void
test_pattern(const test_spec *spec)
```
## Detailed Description
The  function performs extensive testing of the PostgreSQL IntegerSet data structure using predefined test specifications that define repeating patterns. The function creates an IntegerSet, populates it with values according to a specified bit pattern, and then validates the correctness of various IntegerSet operations including membership testing, iteration, and memory management.

The function works by processing a pattern string where '1' characters represent positions where integers should be added to the set. It creates these patterns with specified spacing and fills the set with the appropriate values. After population, it performs comprehensive validation through random membership probes and full iteration to ensure data integrity. The function also tracks and reports performance metrics including execution times and memory usage statistics.

## Parameters / Member Variables
- : Pointer to a test_spec structure containing:
  - : Name identifier for the test pattern
  - : String representing the bit pattern ('1' = include, '0' = exclude)
  - : Total number of values to add to the set
  - : Interval between pattern repetitions

## Dependencies
- Functions called/Symbols referenced:
  - [IntegerSet](../I/IntegerSet.md) (data structure)
  - [intset_create](../i/intset_create.md)
  - [intset_add_member](../i/intset_add_member.md)
  - [intset_is_member](../i/intset_is_member.md)
  - [intset_num_entries](../i/intset_num_entries.md)
  - [intset_begin_iterate](../i/intset_begin_iterate.md)
  - [intset_iterate_next](../i/intset_iterate_next.md)
  - [intset_memory_usage](../i/intset_memory_usage.md)
  - AllocSetContextCreate
  - [MemoryContextSetIdentifier](../M/MemoryContextSetIdentifier.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - [MemoryContextStats](../M/MemoryContextStats.md)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [pg_prng_uint64_range](../p/pg_prng_uint64_range.md)
  - UINT64_FORMAT
- Called from (representative examples):
  - [test_integerset](test_integerset.md) (main test entry point)

## Notes and Other Information
- This is a static function, only accessible within the test_integerset.c file
- Creates separate memory contexts for precise memory usage tracking
- Performs 100,000 random membership probes for thorough validation
- Includes comprehensive performance timing for insertion, lookup, and iteration operations
- Reports detailed memory usage statistics when intset_test_stats is enabled
- Validates both the correctness of stored values and the completeness of iteration
- Uses pattern-based testing to create predictable but diverse data distributions
- Located in: src/test/modules/test_integerset/test_integerset.c:135-320
- Essential component of PostgreSQL's IntegerSet validation suite

## Simplified Source

```c
static void test_pattern(const test_spec *spec) {
    IntegerSet *intset;
    MemoryContext intset_ctx;
    MemoryContext old_ctx;
    TimestampTz starttime, endtime;
    uint64 n, last_int;
    int patternlen;
    uint64 *pattern_values;
    uint64 pattern_num_values;

    elog(NOTICE, "testing intset with pattern \"%s\"", spec->test_name);

    // Pre-process the pattern string into an array of positions
    patternlen = strlen(spec->pattern_str);
    pattern_values = palloc(patternlen * sizeof(uint64));
    pattern_num_values = 0;
    for (int i = 0; i < patternlen; i++) {
        if (spec->pattern_str[i] == '1')
            pattern_values[pattern_num_values++] = i;
    }

    // Create IntegerSet in separate memory context for tracking
    intset_ctx = AllocSetContextCreate(CurrentMemoryContext,
                                      "intset test", ALLOCSET_SMALL_SIZES);
    MemoryContextSetIdentifier(intset_ctx, spec->test_name);
    old_ctx = MemoryContextSwitchTo(intset_ctx);
    intset = intset_create();
    MemoryContextSwitchTo(old_ctx);

    // Add values to the set following the pattern
    starttime = GetCurrentTimestamp();
    n = 0;
    last_int = 0;
    while (n < spec->num_values) {
        for (int i = 0; i < pattern_num_values && n < spec->num_values; i++) {
            uint64 x = last_int + pattern_values[i];
            intset_add_member(intset, x);
            n++;
        }
        last_int += spec->spacing;
    }
    endtime = GetCurrentTimestamp();

    if (intset_test_stats) {
        fprintf(stderr, "added " UINT64_FORMAT " values in %d ms\n",
                spec->num_values, (int) (endtime - starttime) / 1000);

        // Print memory usage stats
        uint64 mem_usage = intset_memory_usage(intset);
        fprintf(stderr, "intset_memory_usage() reported " UINT64_FORMAT
                " (%0.2f bytes / integer)\n",
                mem_usage, (double) mem_usage / spec->num_values);
        MemoryContextStats(intset_ctx);
    }

    // Verify count matches expected
    n = intset_num_entries(intset);
    if (n != spec->num_values)
        elog(ERROR, "intset_num_entries returned " UINT64_FORMAT
             ", expected " UINT64_FORMAT, n, spec->num_values);

    // Test random membership probes
    starttime = GetCurrentTimestamp();
    for (n = 0; n < 100000; n++) {
        uint64 x = pg_prng_uint64_range(&pg_global_prng_state, 0, last_int + 1000);

        // Calculate expected result based on pattern
        bool expected;
        if (x >= last_int) {
            expected = false;
        } else {
            uint64 idx = x % spec->spacing;
            expected = (idx < patternlen && spec->pattern_str[idx] == '1');
        }

        bool actual = intset_is_member(intset, x);
        if (actual != expected)
            elog(ERROR, "mismatch at " UINT64_FORMAT ": %d vs %d", x, actual, expected);
    }
    endtime = GetCurrentTimestamp();

    if (intset_test_stats)
        fprintf(stderr, "probed " UINT64_FORMAT " values in %d ms\n",
                n, (int) (endtime - starttime) / 1000);

    // Test iteration through all values
    starttime = GetCurrentTimestamp();
    intset_begin_iterate(intset);
    n = 0;
    last_int = 0;
    while (n < spec->num_values) {
        for (int i = 0; i < pattern_num_values && n < spec->num_values; i++) {
            uint64 expected = last_int + pattern_values[i];
            uint64 x;

            if (!intset_iterate_next(intset, &x))
                break;

            if (x != expected)
                elog(ERROR, "iterate returned wrong value; got " UINT64_FORMAT
                     ", expected " UINT64_FORMAT, x, expected);
            n++;
        }
        last_int += spec->spacing;
    }
    endtime = GetCurrentTimestamp();

    if (intset_test_stats)
        fprintf(stderr, "iterated " UINT64_FORMAT " values in %d ms\n",
                n, (int) (endtime - starttime) / 1000);

    // Verify iteration completeness
    if (n != spec->num_values)
        elog(ERROR, "iterator returned " UINT64_FORMAT " entries, "
             UINT64_FORMAT " was expected", n, spec->num_values);

    MemoryContextDelete(intset_ctx);
}
```