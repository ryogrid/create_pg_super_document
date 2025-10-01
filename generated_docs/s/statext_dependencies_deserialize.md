# statext_dependencies_deserialize

## Location
[src/backend/statistics/dependencies.c:499-594](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/dependencies.c#L499-L594)

## Overview
Reads serialized functional dependencies from a bytea value and reconstructs the in-memory MVDependencies structure with comprehensive validation and error checking.

## Definition

```c
MVDependencies *
statext_dependencies_deserialize(bytea *data)
```
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

## Simplified Source

```c
MVDependencies *
statext_dependencies_deserialize(bytea *data)
{
    int i;
    Size min_expected_size;
    MVDependencies *dependencies;
    char *tmp;

    if (data == NULL)
        return NULL;

    // Basic size validation
    if (VARSIZE_ANY_EXHDR(data) < SizeOfHeader)
        elog(ERROR, "invalid MVDependencies size %zu (expected at least %zu)",
             VARSIZE_ANY_EXHDR(data), SizeOfHeader);

    // Allocate base structure and initialize data pointer
    dependencies = (MVDependencies *) palloc0(sizeof(MVDependencies));
    tmp = VARDATA_ANY(data);

    // Read and validate header fields
    memcpy(&dependencies->magic, tmp, sizeof(uint32));
    tmp += sizeof(uint32);
    memcpy(&dependencies->type, tmp, sizeof(uint32));
    tmp += sizeof(uint32);
    memcpy(&dependencies->ndeps, tmp, sizeof(uint32));
    tmp += sizeof(uint32);

    // Validate header values
    if (dependencies->magic != STATS_DEPS_MAGIC)
        elog(ERROR, "invalid dependency magic %d (expected %d)",
             dependencies->magic, STATS_DEPS_MAGIC);
    if (dependencies->type != STATS_DEPS_TYPE_BASIC)
        elog(ERROR, "invalid dependency type %d (expected %d)",
             dependencies->type, STATS_DEPS_TYPE_BASIC);
    if (dependencies->ndeps == 0)
        elog(ERROR, "invalid zero-length item array in MVDependencies");

    // Check minimum expected size
    min_expected_size = SizeOfItem(dependencies->ndeps);
    if (VARSIZE_ANY_EXHDR(data) < min_expected_size)
        elog(ERROR, "invalid dependencies size %zu (expected at least %zu)",
             VARSIZE_ANY_EXHDR(data), min_expected_size);

    // Reallocate to include space for dependency array
    dependencies = repalloc(dependencies, offsetof(MVDependencies, deps)
                           + (dependencies->ndeps * sizeof(MVDependency *)));

    // Deserialize each dependency
    for (i = 0; i < dependencies->ndeps; i++) {
        double degree;
        AttrNumber k;
        MVDependency *d;

        // Read dependency metadata
        memcpy(&degree, tmp, sizeof(double));
        tmp += sizeof(double);
        memcpy(&k, tmp, sizeof(AttrNumber));
        tmp += sizeof(AttrNumber);

        Assert((k >= 2) && (k <= STATS_MAX_DIMENSIONS));

        // Allocate and initialize dependency structure
        d = (MVDependency *) palloc0(offsetof(MVDependency, attributes)
                                    + (k * sizeof(AttrNumber)));
        d->degree = degree;
        d->nattributes = k;

        // Copy attribute numbers
        memcpy(d->attributes, tmp, sizeof(AttrNumber) * d->nattributes);
        tmp += sizeof(AttrNumber) * d->nattributes;

        dependencies->deps[i] = d;
        Assert(tmp <= ((char *) data + VARSIZE_ANY(data)));
    }

    // Ensure we consumed exactly all data
    Assert(tmp == ((char *) data + VARSIZE_ANY(data)));

    return dependencies;
}
```