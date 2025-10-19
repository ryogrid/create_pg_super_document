# setup_collation

## Location
[src/bin/initdb/initdb.c:1753-1785](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L1753-L1785)

## Overview
The  function initializes and populates the PostgreSQL collation system by setting up collation versions and importing system collations during database cluster initialization.

## Definition

```c
static void
setup_collation(FILE *cmdfd)
```
## Detailed Description
This function is responsible for configuring PostgreSQL's collation support, which is essential for proper text sorting, comparison, and internationalization. It performs two critical operations:

1. **Collation Version Management**: Updates the version information for predefined collations in , specifically targeting the 'unicode' collation. This ensures that PostgreSQL tracks the version of collation libraries to detect potential changes that could affect data ordering consistency.

2. **System Collation Import**: Executes  to discover and import all available collations from the operating system's locale system. This makes the full range of system-supported collations available for use within PostgreSQL databases.

The function focuses on collations where the behavior might change over time (hence version tracking is important) while avoiding unnecessary version updates for collations known to have stable behavior. This approach optimizes both functionality and performance.

## Parameters / Member Variables
- `*cmdfd`: FILE pointer to the command file descriptor where SQL commands are written for execution
## Dependencies
- Functions called/Symbols referenced:
  - : Macro for writing SQL commands to the command file descriptor
- Called from (representative examples):
  - : Main database initialization sequence
  - : Authentication configuration context

## Notes and Other Information
- The function specifically targets the 'unicode' collation for version tracking, indicating its importance and potential for behavioral changes across different ICU library versions
- The  function is used to determine the current version of collation implementations
- The  function is called with the 'pg_catalog' schema, ensuring system collations are properly cataloged
- This function is critical for internationalization support in PostgreSQL, enabling proper text handling across different languages and locales
- The separation of version management and system import operations allows for fine-grained control over collation initialization
- Double newlines (\n\n) provide formatting separation in the generated SQL script
- The function assumes the existence of  which contains predefined collation definitions
- System collation import makes PostgreSQL aware of all locale-specific collations available on the host operating system

## Simplified Source

```c
static void
setup_collation(FILE *cmdfd)
{
    // Set collation version for unicode collation to track library changes
    PG_CMD_PUTS("UPDATE pg_collation SET collversion = pg_collation_actual_version(oid) WHERE collname = 'unicode';\n\n");

    // Import all available system collations from the operating system
    PG_CMD_PUTS("SELECT pg_import_system_collations('pg_catalog');\n\n");
}
```