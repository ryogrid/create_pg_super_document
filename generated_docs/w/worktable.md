# worktable

## Location
[src/test/modules/worker_spi/worker_spi.c:62-66](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/worker_spi/worker_spi.c#L62-L66)

## Overview
The `worktable` struct is a simple data structure used in PostgreSQL's worker_spi test module to represent schema and table information for background worker processes that demonstrate SPI (Server Programming Interface) usage.

## Definition
```c
typedef struct worktable
{
    const char *schema;
    const char *name;
} worktable;
```

## Detailed Description
The `worktable` struct is defined in the worker_spi test module and serves as a container for schema and table identifiers used by background worker processes. This struct is part of a demonstration module that shows how to create background workers that can connect to databases, execute SQL commands through SPI, and perform database operations.

The worker_spi module creates background workers that:
1. Connect to a specified database
2. Create a schema and table if they don't exist
3. Process "delta" type rows by aggregating their values into a "total" type row
4. Delete the processed delta rows

Each worker instance uses a unique schema name (schema1, schema2, etc.) and a common table name ("counted") to avoid conflicts when multiple workers are running simultaneously.

## Parameters / Member Variables
- `schema`: A pointer to a constant character string containing the schema name. This is typically set to a dynamically generated name like "schema1", "schema2", etc., based on the worker index.
- `name`: A pointer to a constant character string containing the table name. This is typically set to "counted" for all worker instances.

## Dependencies
- Functions called/Symbols referenced:
  - No direct symbol references (struct definition only)
- Called from (representative examples):
  - [initialize_worker_spi](../i/initialize_worker_spi.md) at src/test/modules/worker_spi/worker_spi.c:73
  - [worker_spi_main](worker_spi_main.md) at src/test/modules/worker_spi/worker_spi.c:141
  - [worker_spi_main](worker_spi_main.md) at src/test/modules/worker_spi/worker_spi.c:149

## Notes and Other Information
- This struct is part of the PostgreSQL test infrastructure, specifically designed to demonstrate background worker functionality
- The schema and name fields are typically allocated using `pstrdup()` to ensure proper memory management
- After initialization, the identifiers may be quoted using `quote_identifier()` to handle special characters or reserved words
- The struct is used in conjunction with SPI functions to create and manipulate database objects
- The worktable represents a specific pattern where each background worker operates on its own schema to avoid conflicts
- Memory for the struct instance is typically allocated using `palloc()`