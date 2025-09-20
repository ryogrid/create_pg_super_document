# pg_dependencies_send

## Location
[src/backend/statistics/dependencies.c:726-740](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/dependencies.c#L726-L740)

## Overview
This function serves as the binary output routine for the pg_dependencies data type, enabling transmission of dependency statistics over PostgreSQL's binary protocol by delegating to the bytea send function.

## Definition

```c
Datum
pg_dependencies_send(PG_FUNCTION_ARGS)
```
## Detailed Description
The function handles binary output of pg_dependencies values for PostgreSQL's binary communication protocol. Since functional dependencies are internally stored as serialized bytea data, the function simply delegates to the existing byteasend() function rather than implementing custom binary serialization.

This design leverages the fact that pg_dependencies data is already in binary form within PostgreSQL's storage system. By reusing byteasend(), the function efficiently transmits the raw binary dependency data without additional processing overhead, maintaining both performance and correctness.

The function is automatically invoked when pg_dependencies values need to be sent over the binary protocol, such as during query result transmission or prepared statement parameter binding.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro (standard PostgreSQL function calling convention)
- Implicitly receives fcinfo (function call info) containing the pg_dependencies value
- Returns binary representation as Datum

## Dependencies
- Functions called/Symbols referenced:
  - [byteasend](../b/byteasend.md) (PostgreSQL's standard binary output function for bytea type)

- Called from (representative examples):
  - Not directly called (registered as type binary output function in system catalogs)

## Notes and Other Information
- Complements pg_dependencies_out for binary protocol support
- Leverages existing bytea infrastructure for efficient binary transmission
- Part of PostgreSQL's complete type system implementation for pg_dependencies
- Unlike the input functions, this allows output since reading existing data is safe
- Enables pg_dependencies values to be transmitted via binary protocol connections
- Essential for client applications using prepared statements or binary result formats