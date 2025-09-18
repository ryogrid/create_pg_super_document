# statext_dependencies_deserialize

## Location
[src/backend/statistics/dependencies.c:499-594](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/dependencies.c#L499-L594)

## Overview
Reads serialized functional dependencies from a bytea value and reconstructs the in-memory MVDependencies structure with comprehensive validation and error checking.

## Definition


## Detailed Description
This function performs the reverse operation of statext_dependencies_serialize, converting a serialized bytea representation back into a fully functional MVDependencies structure. The deserialization process includes:

1. Validates input data is not NULL and has minimum required size
2. Reads and validates the header (magic number, type, dependency count)  
3. Performs extensive sanity checks on all header values
4. Calculates expected minimum size based on dependency count
5. Allocates memory for the MVDependencies structure and dependency array
6. For each dependency, reads degree, attribute count, and attribute numbers
7. Validates attribute counts are within acceptable bounds (2 to STATS_MAX_DIMENSIONS)
8. Ensures exact consumption of all bytea data with no overflow or underflow

The function includes comprehensive error handling with detailed error messages for various failure scenarios.

## Parameters / Member Variables
- : bytea containing the serialized functional dependencies data (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - VARSIZE_ANY_EXHDR (macro for getting bytea data size)
  - VARDATA_ANY (macro for getting bytea data pointer)
  - SizeOfHeader (macro for header size validation)
  - SizeOfItem (macro for item size calculation)
  - STATS_DEPS_MAGIC (validation constant)
  - STATS_DEPS_TYPE_BASIC (type validation constant)
  - STATS_MAX_DIMENSIONS (maximum attribute limit)
  - [palloc0](../p/palloc0.md) (PostgreSQL memory allocation)
  - [repalloc](../r/repalloc.md) (PostgreSQL memory reallocation)
  - memcpy (system memory copy function)
  - elog (PostgreSQL error logging)
- Called from:
  - [statext_dependencies_load](statext_dependencies_load.md)
  - [pg_dependencies_out](../p/pg_dependencies_out.md)

## Notes and Other Information
- Returns NULL if input data is NULL, allowing for graceful handling of missing statistics
- Performs strict validation of magic numbers and type identifiers to detect data corruption
- Uses assertions for development-time validation and elog for runtime error reporting
- Validates that attribute counts are within reasonable bounds (2 to STATS_MAX_DIMENSIONS)
- Memory allocation strategy first allocates base structure, then reallocates to include dependency array
- Ensures exact bytea consumption to detect serialization format mismatches
- Part of PostgreSQL's extended statistics persistence and loading mechanism
- Critical for maintaining data integrity when loading functional dependency statistics from disk