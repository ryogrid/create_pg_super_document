# ecpg_prepared

## Location
[src/interfaces/ecpg/ecpglib/prepare.c:357-367](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/prepare.c#L357-L367)

## Overview
A utility function that retrieves the SQL command text associated with a named prepared statement in the ECPG library.

## Definition

```c
char *
ecpg_prepared(const char *name, struct connection *con)
```
## Detailed Description
 provides a lookup mechanism to retrieve the original SQL command text from a prepared statement by its name within a specific database connection. The function searches through the connection's list of prepared statements to find the named statement and returns a pointer to the stored command string. This functionality is useful for debugging, logging, or when applications need to inspect the SQL commands associated with their prepared statements. The function returns a direct pointer to the internal command string, so the caller should not modify or free the returned memory.

## Parameters / Member Variables
- `*name`: Name of the prepared statement whose command text should be retrieved
- `*con`: Pointer to the database connection structure containing the prepared statements
## Dependencies
- Functions called/Symbols referenced:
  - [ecpg_find_prepared_statement](ecpg_find_prepared_statement.md) (locate the named prepared statement in the connection's list)
  - [prepared_statement](../p/prepared_statement.md) (structure type for prepared statement data)
- Called from (representative examples):
  - [ecpg_do_prologue](ecpg_do_prologue.md) (during statement execution setup)
  - [ECPGprepared_statement](../E/ECPGprepared_statement.md) (public API for retrieving prepared statement information)

## Notes and Other Information
- Returns a pointer to the SQL command string if the prepared statement is found, NULL if not found
- The returned pointer references internal memory and should not be modified or freed by the caller
- Used primarily for introspection and debugging purposes within the ECPG library
- The function performs a simple lookup without any side effects on the prepared statement
- Part of the internal ECPG API, primarily used by other ECPG library functions
- Essential for statement execution when the original command text needs to be referenced or logged

## Simplified Source

```c
char *ecpg_prepared(const char *name, struct connection *con) {
    // Find the prepared statement by name in the connection
    struct prepared_statement *this = ecpg_find_prepared_statement(name, con, NULL);

    // Return the command text if found, NULL otherwise
    return this ? this->stmt->command : NULL;
}
```