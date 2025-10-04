# SPI_register_relation

## Location
[src/backend/executor/spi.c:3297-3330](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L3297-L3330)

## Overview
Registers an ephemeral named relation for use by the planner and executor on subsequent calls using the current SPI connection.

## Definition

```c
int
SPI_register_relation(EphemeralNamedRelation enr)
```
## Detailed Description
This function is part of the SPI (Server Programming Interface) API that allows registration of ephemeral named relations (ENRs) within the current SPI execution context. The function validates the input parameter, checks for duplicate registrations using the relation's name, and if no duplicate exists, registers the ENR in the query environment. If no query environment exists yet, it creates one. The function uses SPI's begin/end call mechanism to manage the execution context properly.

## Parameters / Member Variables
- `enr`: Pointer to an EphemeralNamedRelation structure containing the relation to be registered. Must not be NULL and must have a valid name in enr->md.name.
## Dependencies
- Functions called/Symbols referenced:
  - [_SPI_begin_call](_SPI_begin_call.md) (to start SPI call context)
  - [_SPI_find_ENR_by_name](_SPI_find_ENR_by_name.md) (to check for duplicate names)
  - [create_queryEnv](../c/create_queryEnv.md) (to create query environment if needed)
  - [register_ENR](../r/register_ENR.md) (to actually register the relation)
  - [_SPI_end_call](_SPI_end_call.md) (to end SPI call context)
- Called from (representative examples):
  - [SPI_register_trigger_data](SPI_register_trigger_data.md)

## Notes and Other Information
- Returns SPI_ERROR_ARGUMENT if enr is NULL or enr->md.name is NULL
- Returns SPI_ERROR_REL_DUPLICATE if a relation with the same name already exists
- Returns SPI_OK_REL_REGISTER on successful registration
- The function preserves the current memory context by passing false to _SPI_begin_call
- Creates a query environment lazily if one doesn't already exist
- Part of the public SPI API for managing ephemeral named relations

## Simplified Source

```c
int SPI_register_relation(EphemeralNamedRelation enr) {
    // Validate input parameters
    if (enr == NULL || enr->md.name == NULL)
        return SPI_ERROR_ARGUMENT;

    // Begin SPI call context
    int res = _SPI_begin_call(false);
    if (res < 0)
        return res;

    // Check for duplicate relation name
    EphemeralNamedRelation match = _SPI_find_ENR_by_name(enr->md.name);
    if (match) {
        res = SPI_ERROR_REL_DUPLICATE;
    } else {
        // Create query environment if it doesn't exist
        if (_SPI_current->queryEnv == NULL)
            _SPI_current->queryEnv = create_queryEnv();

        // Register the ephemeral named relation
        register_ENR(_SPI_current->queryEnv, enr);
        res = SPI_OK_REL_REGISTER;
    }

    // End SPI call context
    _SPI_end_call(false);
    return res;
}
```