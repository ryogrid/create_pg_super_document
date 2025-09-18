# ECPGdescribe

## Location
[src/interfaces/ecpg/ecpglib/descriptor.c:847-991](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/descriptor.c#L847-L991)

## Overview
Retrieves metadata information about a prepared statement and stores it in the specified descriptor or SQLDA structure.

## Definition
```c
bool ECPGdescribe(int line, int compat, bool input, const char *connection_name, const char *stmt_name, ...)
```

## Detailed Description
This function implements the SQL DESCRIBE statement functionality for ECPG. It retrieves metadata about the output columns of a prepared statement using PostgreSQL's `PQdescribePrepared()` function. The function supports two main output formats: SQL descriptors (ECPGt_descriptor) and SQLDA structures (ECPGt_sqlda). For SQLDA structures, it handles both Informix-compatible and native PostgreSQL formats. The function uses a variadic argument list to process multiple output targets and performs comprehensive error checking throughout the operation.

## Parameters / Member Variables
- `line`: Line number for error reporting purposes in the ECPG preprocessor context
- `compat`: Compatibility mode flags (e.g., INFORMIX_MODE)
- `input`: Boolean flag indicating whether this is a DESCRIBE INPUT operation (currently unsupported)
- `connection_name`: Name of the database connection to use (NULL for default connection)
- `stmt_name`: Name of the prepared statement to describe
- `...`: Variadic arguments containing output target specifications (type, pointer, and size information)

## Dependencies
- Functions called/Symbols referenced:
  - ecpg_get_connection: Retrieves connection object by name
  - [ecpg_find_prepared_statement](../e/ecpg_find_prepared_statement.md): Finds prepared statement by name
  - ecpg_find_desc: Finds descriptor by name
  - [PQdescribePrepared](../P/PQdescribePrepared.md): PostgreSQL function to get prepared statement metadata
  - [ecpg_check_PQresult](../e/ecpg_check_PQresult.md): Checks PostgreSQL result for errors
  - [ecpg_build_compat_sqlda](../e/ecpg_build_compat_sqlda.md): Builds Informix-compatible SQLDA structure
  - [ecpg_build_native_sqlda](../e/ecpg_build_native_sqlda.md): Builds native PostgreSQL SQLDA structure
  - [PQclear](../P/PQclear.md): Frees PostgreSQL result structures
  - [ecpg_raise](../e/ecpg_raise.md): Raises ECPG errors with appropriate error codes
  - ecpg_gettext: Internationalization function for error messages
  - INFORMIX_MODE: Macro to check compatibility mode
  - Various ECPG error constants and SQL state codes

- Called from (representative examples):
  - Various test programs in src/interfaces/ecpg/test/expected/
  - ECPG-generated code for DESCRIBE statements

## Notes and Other Information
- Returns `true` on successful description, `false` on error
- DESCRIBE INPUT operations are not currently supported and will raise an ECPG_UNSUPPORTED error
- The function handles both descriptor and SQLDA output targets through a variadic argument interface
- For SQLDA targets, properly manages memory by freeing old SQLDA chains before assigning new ones
- Supports both Informix compatibility mode and native PostgreSQL mode for SQLDA structures
- Error conditions include: unsupported DESCRIBE INPUT, invalid connection, invalid prepared statement, PostgreSQL result errors
- The variadic argument processing follows a specific pattern with multiple parameters per target
- This function is part of the ECPG (Embedded SQL in C) library for PostgreSQL
- Thread-safe when used with different connections and prepared statements
- The function is typically called by ECPG-generated code when processing DESCRIBE SQL statements