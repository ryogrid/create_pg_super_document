# validateSQLNamePattern

## Location
[src/bin/psql/describe.c:6164-6216](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L6164-L6216)

## Overview
A static validation wrapper function that processes and validates SQL name patterns for use in psql describe commands, ensuring proper formatting and preventing SQL injection attacks.

## Definition

```c
static bool
validateSQLNamePattern(PQExpBuffer buf, const char *pattern, bool have_where,
					   bool force_escape, const char *schemavar,
					   const char *namevar, const char *altnamevar,
					   const char *visibilityrule, bool *added_clause,
					   int maxparts)
```
## Detailed Description
This function serves as a validation wrapper around the processSQLNamePattern function from string_utils. It processes SQL name patterns (which may contain wildcards and qualified names) and adds appropriate WHERE clauses to SQL queries while enforcing several important validations:

1. **Dot count validation**: Ensures the pattern doesn't have more dotted components than allowed by maxparts
2. **Cross-database reference prevention**: Prevents attempts to reference objects in different databases
3. **Connection validation**: Ensures the user is connected to a database when using fully qualified names

The function integrates pattern processing with safety checks to prevent common errors and security issues in psql's describe commands.

## Parameters / Member Variables
- `buf`: PQExpBuffer to append the generated WHERE clause to
- `*pattern`: The input pattern string (may contain wildcards, dots for qualification)
- `have_where`: Whether the query already has a WHERE clause (affects AND/WHERE choice)
- `force_escape`: Whether to force escaping of special characters
- `*schemavar`: SQL variable name for the schema column
- `*namevar`: SQL variable name for the object name column
- `*altnamevar`: Alternative name variable (can be NULL)
- `*visibilityrule`: SQL expression for visibility checking (can be NULL)
- `*added_clause`: Output parameter indicating whether a clause was actually added
- `maxparts`: Maximum number of dotted name parts allowed (e.g., 3 for database.schema.table)
## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md) (data structure)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [processSQLNamePattern](../p/processSQLNamePattern.md)
  - [PQdb](../P/PQdb.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
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

## Simplified Source

```c
static bool validateSQLNamePattern(PQExpBuffer buf, const char *pattern, bool have_where,
                                   bool force_escape, const char *schemavar,
                                   const char *namevar, const char *altnamevar,
                                   const char *visibilityrule, bool *added_clause,
                                   int maxparts) {
    PQExpBufferData dbbuf;
    int dotcnt;
    bool added;

    // Process the SQL name pattern
    initPQExpBuffer(&dbbuf);
    added = processSQLNamePattern(pset.db, buf, pattern, have_where, force_escape,
                                  schemavar, namevar, altnamevar,
                                  visibilityrule, &dbbuf, &dotcnt);
    if (added_clause != NULL)
        *added_clause = added;

    // Validate dot count doesn't exceed maximum allowed parts
    if (dotcnt >= maxparts) {
        pg_log_error("improper qualified name (too many dotted names): %s",
                     pattern);
        goto error_return;
    }

    // Check for cross-database references
    if (maxparts > 1 && dotcnt == maxparts - 1) {
        if (PQdb(pset.db) == NULL) {
            pg_log_error("You are currently not connected to a database.");
            goto error_return;
        }
        if (strcmp(PQdb(pset.db), dbbuf.data) != 0) {
            pg_log_error("cross-database references are not implemented: %s",
                         pattern);
            goto error_return;
        }
    }

    termPQExpBuffer(&dbbuf);
    return true;

error_return:
    termPQExpBuffer(&dbbuf);
    return false;
}
```