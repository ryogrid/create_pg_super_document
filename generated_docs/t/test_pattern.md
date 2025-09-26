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