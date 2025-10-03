# pgstat_get_kind_info

## Location
[src/backend/utils/activity/pgstat.c:1265-1278](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L1265-L1278)

## Overview
Returns a pointer to the kind information structure for the specified PostgreSQL statistics kind, providing access to metadata about how statistics are managed for that particular object type.

## Definition

```c
const PgStat_KindInfo *
pgstat_get_kind_info(PgStat_Kind kind)
```
## Detailed Description
This function serves as a simple accessor to retrieve the  structure for a given statistics kind. The  structure contains metadata about how statistics are handled for different PostgreSQL object types (databases, tables, functions, etc.). The function validates the input kind using an assertion and then returns a pointer to the corresponding entry in the global  array.

The function is a core utility in PostgreSQL's statistics infrastructure, providing a centralized way to access configuration information about different statistics kinds. This information is used throughout the statistics system to determine how to handle creation, deletion, serialization, and other operations for statistics entries.

## Parameters / Member Variables
- `kind`: A  enum value specifying which type of statistics object to get information for (e.g., database, table, function statistics)
## Dependencies
- Functions called/Symbols referenced:
  - : Validates that the provided kind is valid
  - : Global array containing kind information structures
  - : Enum type for statistics kinds

- Called from (representative examples):
  - : For resetting statistics
  - : For retrieving statistics entries
  - : For building statistics snapshots
  - : For writing statistics to file
  - : For initializing new statistics entries

## Notes and Other Information
- The function uses an assertion to validate the input, so it should only be called with valid  values
- Returns a const pointer, indicating that the caller should not modify the returned structure
- This is a lightweight accessor function with minimal overhead
- The returned pointer is valid for the lifetime of the process as it points to static data

## Simplified Source

```c
// Simplified version of pgstat_get_kind_info
const PgStat_KindInfo *pgstat_get_kind_info(PgStat_Kind kind) {
    // Return pointer to the kind information structure
    return &pgstat_kind_infos[kind];
}
```

Key simplifications made:
- Removed validation assertion for clarity
- Simple array access operation
- Core functionality preserved: retrieves kind-specific metadata
- Essential lookup function for statistics infrastructure