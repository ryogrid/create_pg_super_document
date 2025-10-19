# getFailures

## Location
[src/bin/pgbench/pgbench.c:4519-4529](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L4519-L4529)

## Overview
Returns the total number of failed transactions by summing serialization failures and deadlock failures from the provided statistics data.

## Definition

```c
static int64
getFailures(const StatsData *stats)
```
## Detailed Description
This function calculates the total number of transaction failures in pgbench by aggregating two specific types of failures: serialization failures and deadlock failures. It provides a consolidated count of all transaction failures for reporting purposes. The function is used internally within pgbench for progress reporting and final results display.

## Parameters / Member Variables
- `stats`: Pointer to a StatsData structure containing transaction statistics including failure counts

## Dependencies
- Functions called/Symbols referenced:
  - [StatsData](../S/StatsData.md) (structure type)
- Called from (representative examples):
  - [printProgressReport](../p/printProgressReport.md) (at src/bin/pgbench/pgbench.c:6306)
  - [printResults](../p/printResults.md) (at src/bin/pgbench/pgbench.c:6396, 6515)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the pgbench.c file
- Returns an int64 value to accommodate potentially large failure counts
- The function simply adds two failure counters: serialization_failures and deadlock_failures from the StatsData structure
- Used in both progress reporting during benchmark execution and final results reporting

## Simplified Source
```c
static int64 getFailures(const StatsData *stats) {
    // Sum all transaction failure types
    return (stats->serialization_failures + stats->deadlock_failures);
}
```