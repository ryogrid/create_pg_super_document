# libpqrcv_check_conninfo

## Location
src/backend/replication/libpqwalreceiver/libpqwalreceiver.c: 316 - 370

## Overview
Validates PostgreSQL connection string syntax and enforces password requirements for security compliance in replication connections.

## Definition
```c
static void libpqrcv_check_conninfo(const char *conninfo, bool must_use_password)
```

## Detailed Description
The `libpqrcv_check_conninfo` function performs validation of PostgreSQL connection strings before they are used for establishing replication connections. It serves two primary purposes: syntax validation and security enforcement.

The function first parses the connection string using libpq's `PQconninfoParse` to verify that it has valid syntax. If parsing fails, it reports a syntax error and terminates execution.

When `must_use_password` is true (typically for non-superuser connections), the function also enforces that a non-empty password is explicitly provided in the connection string. This security measure prevents non-privileged users from connecting without authentication, which could occur if the server's authentication method doesn't require a password or if external password sources (like .pgpass files) are used.

## Parameters / Member Variables
- `conninfo`: Connection string or URI to validate (must not be NULL)
- `must_use_password`: If true, function enforces that a password is explicitly provided in the connection string

## Dependencies
- Functions called/Symbols referenced:
  - [PQconninfoParse](../P/PQconninfoParse.md) (parse and validate connection string syntax)
  - [PQconninfoFree](../P/PQconninfoFree.md) (free parsed connection options)
  - [PQfreemem](../P/PQfreemem.md) (free libpq-allocated error messages)
  - [pstrdup](../p/pstrdup.md) (duplicate error message string)
  - `ereport` (report errors with appropriate error codes)

- Called from (representative examples):
  - [libpqrcv_connect](libpqrcv_connect.md) (before establishing connections)
  - Registered in `PQWalReceiverFunctions` table as `walrcv_check_conninfo`
  - Used during subscription setup and validation

## Notes and Other Information
- Function does not return a value; it either succeeds silently or reports an ERROR
- Uses libpq's native connection string parsing for comprehensive validation
- Enforces explicit password specification when `must_use_password` is true to prevent security bypasses
- Properly manages memory by freeing libpq-allocated structures and error messages
- Error messages are appropriately categorized with SQL standard error codes
- Validation occurs before actual connection attempts to catch issues early
- Security check prevents non-superusers from relying on external password sources
- Function is designed to be called multiple times safely with the same connection string