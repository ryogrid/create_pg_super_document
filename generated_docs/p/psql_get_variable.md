# psql_get_variable

## Location
[src/bin/psql/common.c:176-266](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L176-L266)

## Overview
Variable-fetching callback function for the flex lexer that retrieves psql variables with optional quoting and escaping for different contexts.

## Definition
```c
char *psql_get_variable(const char *varname, PsqlScanQuoteType quote, void *passthrough)
```

## Detailed Description
This function serves as the interface between psql's lexer and its variable system. It retrieves variable values and applies appropriate quoting and escaping based on the target context. The function supports several key features:

1. **Conditional execution**: Respects the \if conditional stack to suppress variable expansion in inactive branches
2. **Multiple quoting modes**: Handles plain text, SQL literals, SQL identifiers, and shell arguments with context-appropriate escaping
3. **Connection-aware escaping**: Uses the active database connection's encoding for SQL quoting operations
4. **Error handling**: Provides detailed error messages for escaping failures and missing connections

The function allocates memory for the returned string, which the caller must free. It returns NULL for non-existent variables, inactive conditional branches, or escaping errors.

## Parameters / Member Variables
- `varname`: Name of the variable to retrieve from the psql variable system
- `quote`: Quoting type specifying how to format the returned value (PQUOTE_PLAIN, PQUOTE_SQL_LITERAL, PQUOTE_SQL_IDENT, or PQUOTE_SHELL_ARG)
- `passthrough`: Pointer to ConditionalStack for checking if variable expansion is allowed in current context

## Dependencies
- Functions called/Symbols referenced:
  - [conditional_active](../c/conditional_active.md) (to check if in active \if branch)
  - [GetVariable](../G/GetVariable.md) (to retrieve variable value from pset.vars)
  - [pg_strdup](pg_strdup.md) (for plain string duplication)
  - [PQescapeLiteral](../P/PQescapeLiteral.md) (for SQL literal escaping)
  - [PQescapeIdentifier](../P/PQescapeIdentifier.md) (for SQL identifier escaping)
  - [PQfreemem](../P/PQfreemem.md) (to free libpq-allocated memory)
  - [appendShellStringNoError](../a/appendShellStringNoError.md) (for shell argument escaping)
  - [initPQExpBuffer](../i/initPQExpBuffer.md) (for buffer initialization)
  - pg_log_error/pg_log_info (for error reporting)
- Called from (representative examples):
  - Used as callback by flex lexer during variable substitution

## Notes and Other Information
- Returns malloc'd string that caller must free, or NULL on error/non-existence
- SQL escaping requires an active database connection for proper encoding handling
- Shell argument escaping rejects values containing newlines or carriage returns for security
- [Variable](../V/Variable.md) expansion is suppressed entirely in inactive \if conditional branches
- The function is encoding-aware for SQL contexts but encoding-agnostic for shell contexts
- Memory management involves extra strdup() calls to maintain consistent free() semantics across different libpq functions