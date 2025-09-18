# validateSQLNamePattern

## Location
src/bin/psql/describe.c: 6164 - 6216

## Overview
A static validation wrapper function that processes and validates SQL name patterns for use in psql describe commands, ensuring proper formatting and preventing SQL injection attacks.

## Definition


## Detailed Description
This function serves as a validation wrapper around the processSQLNamePattern function from string_utils. It processes SQL name patterns (which may contain wildcards and qualified names) and adds appropriate WHERE clauses to SQL queries while enforcing several important validations:

1. **Dot count validation**: Ensures the pattern doesn't have more dotted components than allowed by maxparts
2. **Cross-database reference prevention**: Prevents attempts to reference objects in different databases
3. **Connection validation**: Ensures the user is connected to a database when using fully qualified names

The function integrates pattern processing with safety checks to prevent common errors and security issues in psql's describe commands.

## Parameters / Member Variables
- : PQExpBuffer to append the generated WHERE clause to
- : The input pattern string (may contain wildcards, dots for qualification)
- : Whether the query already has a WHERE clause (affects AND/WHERE choice)
- : Whether to force escaping of special characters
- : SQL variable name for the schema column
- : SQL variable name for the object name column
- : Alternative name variable (can be NULL)
- : SQL expression for visibility checking (can be NULL)
- : Output parameter indicating whether a clause was actually added
- : Maximum number of dotted name parts allowed (e.g., 3 for database.schema.table)

## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md) (data structure)
  - initPQExpBuffer
  - [processSQLNamePattern](../p/processSQLNamePattern.md)
  - [PQdb](../P/PQdb.md)
  - termPQExpBuffer
- Called from (representative examples):
  - [describeAggregates](../d/describeAggregates.md) (src/bin/psql/describe.c:111)
  - [listForeignTables](../l/listForeignTables.md) (src/bin/psql/describe.c:5971)
  - [listExtensions](../l/listExtensions.md) (src/bin/psql/describe.c:6021)
  - Many other describe functions throughout describe.c

## Notes and Other Information
- This is a static function, only accessible within the describe.c source file
- Critical security function that prevents SQL injection by validating and properly escaping patterns
- Used extensively throughout psql's describe functionality (\d commands)
- Implements PostgreSQL's policy against cross-database references
- Provides user-friendly error messages for common pattern validation failures
- The function handles memory management properly with error_return cleanup
- Returns false on validation failure and true on success
- The maxparts parameter varies depending on the type of object being described (1 for simple names, 2 for schema.name, 3 for database.schema.name)
- Essential for maintaining the security and robustness of psql's pattern matching capabilities