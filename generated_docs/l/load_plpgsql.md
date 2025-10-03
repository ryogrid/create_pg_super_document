# load_plpgsql

## Location
[src/bin/initdb/initdb.c:1974-1982](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L1974-L1982)

## Overview
Installs the PL/pgSQL procedural language extension during database initialization.

## Definition

```c
static void
load_plpgsql(FILE *cmdfd)
```
## Detailed Description
The load_plpgsql function is responsible for installing PL/pgSQL (PostgreSQL Procedural Language/pgSQL) as an extension in a newly initialized database. PL/pgSQL is PostgreSQL's built-in procedural language that extends SQL with programming constructs such as variables, conditional statements, loops, and exception handling.

The function simply generates a "CREATE EXTENSION plpgsql;" SQL command, which instructs PostgreSQL to install the PL/pgSQL language extension. This installation:

- Registers PL/pgSQL as an available procedural language
- Creates the necessary system catalog entries 
- Makes PL/pgSQL available for creating stored procedures, functions, and triggers
- Enables users to write more complex database logic using procedural constructs

PL/pgSQL is considered a core PostgreSQL feature and is typically installed by default in most PostgreSQL databases, making it readily available for application developers who need to implement server-side business logic.

## Parameters / Member Variables
- `*cmdfd`: FILE pointer to the command file where SQL statements are written for execution during database initialization
## Dependencies
- Functions called/Symbols referenced:
  - PG_CMD_PUTS (macro for writing SQL strings to the command file)

- Called from:
  - [initialize_data_directory](../i/initialize_data_directory.md) (main database initialization function)

## Notes and Other Information
- PL/pgSQL is PostgreSQL's most commonly used procedural language
- The extension approach allows for clean installation and uninstallation of the language
- PL/pgSQL supports advanced features like exception handling, cursors, and complex data types
- Installing PL/pgSQL during initdb ensures it's available immediately for user applications
- The language is implemented as a trusted procedural language, meaning non-superusers can create functions in it
- This is typically one of the last steps in database initialization, after core system structures are established