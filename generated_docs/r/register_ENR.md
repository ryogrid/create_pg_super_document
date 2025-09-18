# register_ENR

## Location
src/backend/utils/misc/queryenvironment.c: 69 - 81

## Overview
Registers an ephemeral named relation (ENR) in a query environment, making it available for use during query processing.

## Definition
```c
void register_ENR(QueryEnvironment *queryEnv, EphemeralNamedRelation enr)
```

## Detailed Description
This function adds an ephemeral named relation to a query environment's list of available named relations. It performs validation to ensure the ENR is not NULL and that no relation with the same name is already registered. The function appends the ENR to the namedRelList using PostgreSQL's list manipulation functions. This registration makes the ENR visible and accessible for query planning and execution within the given query environment.

## Parameters / Member Variables
- `queryEnv`: The QueryEnvironment where the ENR should be registered
- `enr`: The EphemeralNamedRelation to register (must not be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - get_ENR (to check for duplicate names)
  - lappend (PostgreSQL list manipulation function)
  - Assert (for parameter validation)
- Called from (representative examples):
  - SPI_register_relation

## Notes and Other Information
- The function includes assertions to validate that enr is not NULL and no duplicate name exists
- If the ENR is intended exclusively for planning purposes, the tstate field can be left NULL
- The ENR is added to the namedRelList which is a PostgreSQL List structure
- This is a mutating operation that modifies the query environment
- Duplicate registrations with the same name will trigger an assertion failure