# SPI_unregister_relation

## Location
[src/backend/executor/spi.c:3331-3363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L3331-L3363)

## Overview
Unregisters an ephemeral named relation by name from the current SPI connection's query environment.

## Definition

```c
int
SPI_unregister_relation(const char *name)
```
## Detailed Description
This function is part of the SPI (Server Programming Interface) API that allows removal of previously registered ephemeral named relations (ENRs) by their string name. The function validates the input parameter, searches for the named relation using the internal lookup function, and if found, removes it from the query environment. This is typically a rarely used function since SPI_finish will automatically clear all registered relations when the SPI connection ends.

## Parameters / Member Variables
- `*name`: A C string containing the name of the ephemeral named relation to unregister. Must not be NULL.
## Dependencies
- Functions called/Symbols referenced:
  - [_SPI_begin_call](_SPI_begin_call.md) (to start SPI call context)
  - [_SPI_find_ENR_by_name](_SPI_find_ENR_by_name.md) (to locate the relation by name)
  - [unregister_ENR](../u/unregister_ENR.md) (to actually remove the relation)
  - [_SPI_end_call](_SPI_end_call.md) (to end SPI call context)
- Called from (representative examples):
  - Part of the public SPI API (referenced in spi.h)

## Notes and Other Information
- Returns SPI_ERROR_ARGUMENT if name is NULL
- Returns SPI_ERROR_REL_NOT_FOUND if no relation with the given name exists
- Returns SPI_OK_REL_UNREGISTER on successful unregistration
- The function preserves the current memory context by passing false to _SPI_begin_call
- This is rarely needed since SPI_finish automatically clears all ENRs
- Part of the public SPI API for managing ephemeral named relations
- Uses the relation's metadata name (match->md.name) when calling unregister_ENR

## Simplified Source

```c
int SPI_unregister_relation(const char *name) {
    // Validate input parameter
    if (name == NULL)
        return SPI_ERROR_ARGUMENT;

    // Begin SPI call context
    int res = _SPI_begin_call(false);
    if (res < 0)
        return res;

    // Find the relation by name
    EphemeralNamedRelation match = _SPI_find_ENR_by_name(name);
    if (match) {
        // Unregister the found relation
        unregister_ENR(_SPI_current->queryEnv, match->md.name);
        res = SPI_OK_REL_UNREGISTER;
    } else {
        res = SPI_ERROR_REL_NOT_FOUND;
    }

    // End SPI call context
    _SPI_end_call(false);
    return res;
}
```