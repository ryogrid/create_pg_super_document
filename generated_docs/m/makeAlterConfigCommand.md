# makeAlterConfigCommand

## Location
[src/bin/pg_dump/dumputils.c:861-930](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/dumputils.c#L861-L930)

## Overview
makeAlterConfigCommand is a helper function for generating ALTER DATABASE/ROLE SET configuration commands during PostgreSQL database dumps.

## Definition

```c
void
makeAlterConfigCommand(PGconn *conn, const char *configitem,
					   const char *type, const char *name,
					   const char *type2, const char *name2,
					   PQExpBuffer buf)
```
## Detailed Description
This function parses a configuration item (in "name=value" format) and wraps it in a complete ALTER command suitable for database dumps. The function handles both simple configuration values and GUC_LIST_QUOTE variables that require special parsing and quoting.

For GUC_LIST_QUOTE variables, the function uses SplitGUCList to parse comma-separated values and then properly quotes each element as a string literal. For regular variables, it treats the entire value as a single string literal.

The generated command follows the pattern: ALTER {DATABASE|ROLE} name [IN {DATABASE|ROLE} name2] SET parameter TO value;

## Parameters / Member Variables
- `*conn`: PostgreSQL connection handle used to determine string-literal quoting conventions
- `*configitem`: A "name=value" string containing the configuration parameter and its value
- `*type`: The type of object being configured ("DATABASE" or "ROLE")
- `*name`: The name of the database or role being configured
- `*type2`: Optional type for IN clause (NULL if not needed)
- `*name2`: Optional name for IN clause (NULL if not needed)
- `buf`: PQExpBuffer where the generated ALTER command will be appended
## Dependencies
- Functions called/Symbols referenced:
  - [pg_strdup](../p/pg_strdup.md): Creates a copy of the configitem string for parsing
  - strchr: Finds the '=' separator in the configuration item
  - [pg_free](../p/pg_free.md): Frees allocated memory
  - [fmtId](../f/fmtId.md): Formats identifiers with proper quoting
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md): Appends formatted text to the buffer
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md): Appends string literals to the buffer
  - [variable_is_guc_list_quote](../v/variable_is_guc_list_quote.md): Checks if a variable uses GUC_LIST_QUOTE format
  - [SplitGUCList](../S/SplitGUCList.md): Parses comma-separated list values for GUC_LIST_QUOTE variables
  - [appendStringLiteralConn](../a/appendStringLiteralConn.md): Appends properly quoted string literals

- Called from (representative examples):
  - [dumpDatabaseConfig](../d/dumpDatabaseConfig.md): Used when dumping database-level configuration settings
  - [dumpUserConfig](../d/dumpUserConfig.md): Used when dumping user/role-level configuration settings

## Notes and Other Information
- The function silently does nothing if it cannot find an '=' in the configitem
- Special handling is required for GUC_LIST_QUOTE variables because their quoting rules differ from standard SQL
- GUC_LIST_QUOTE variables are fully quoted by flatten_set_variable_args() before being stored, but need to be re-parsed for proper SQL generation
- The function is part of the pg_dump utility suite for database backup operations
- Extension variables using GUC_LIST_QUOTE format are not safely supported by this function
- Located at src/bin/pg_dump/dumputils.c:861-930