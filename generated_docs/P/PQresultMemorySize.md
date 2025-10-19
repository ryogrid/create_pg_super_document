# PQresultMemorySize

## Location
[src/interfaces/libpq/fe-exec.c:663-674](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L663-L674)

## Overview
PQresultMemorySize returns the total amount of memory allocated for a PGresult object, providing a way to monitor memory usage.

## Definition
```c
size_t PQresultMemorySize(const PGresult *res)
```

## Detailed Description
PQresultMemorySize is a simple utility function that provides access to the total memory footprint of a PGresult object. It returns the cumulative size of all memory blocks allocated for the result, which is tracked internally by the memory allocation functions. This function is useful for monitoring memory usage and debugging memory-related issues in client applications.

## Parameters / Member Variables
- `res`: Pointer to the PGresult structure to query for memory size

## Dependencies
- Functions called/Symbols referenced:
  - None (direct field access)
- Called from (representative examples):
  - External client applications via libpq-fe.h

## Notes and Other Information
- Returns 0 if the input PGresult pointer is NULL
- The returned size includes all subsidiary storage allocated via pqResultAlloc
- Memory size is tracked incrementally as allocations are made
- This is a read-only operation that does not modify the PGresult
- Located at src/interfaces/libpq/fe-exec.c:663-674

## Simplified Source

```c
size_t PQresultMemorySize(const PGresult *res) {
    // Return 0 if no result object
    if (!res)
        return 0;

    // Return total memory size tracked for this result
    return res->memorySize;
}
```