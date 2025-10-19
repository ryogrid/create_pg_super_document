# parseQuery

## Location
[src/bin/pgbench/pgbench.c:5453-5513](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L5453-L5513)

## Overview
Parses a pgbench SQL command to replace parameter placeholders (:param) with PostgreSQL-style positional parameters (, , etc.).

## Definition

```c
static bool
parseQuery(Command *cmd)
```
## Detailed Description
The parseQuery function processes SQL commands in pgbench by converting named parameter placeholders (e.g., :variable_name) to PostgreSQL's positional parameter format (e.g., , ). This transformation is essential for preparing SQL statements for execution with parameterized queries. The function modifies the command's SQL text in-place and builds an argument array containing the parameter names.

The function iterates through the SQL string looking for ':' characters, then uses parseVariable to extract valid variable names. Each valid parameter is replaced with a positional parameter () and the variable name is stored in the command's argv array for later substitution with actual values.

## Parameters / Member Variables
- `*cmd`: Pointer to Command structure containing the SQL statement to be parsed. The function modifies cmd->lines.data (SQL text), cmd->argv (parameter names), and cmd->argc (parameter count).
## Dependencies
- Functions called/Symbols referenced:
  - [parseVariable](parseVariable.md): Extracts variable names from parameter placeholders
  - [replaceVariable](../r/replaceVariable.md): Performs the actual text replacement in the SQL string
  - [pg_strdup](pg_strdup.md): Duplicates the SQL string for modification
  - [pg_free](pg_free.md): Frees allocated memory on error
  - pg_log_error: Logs error messages
  - MAX_ARGS: Maximum number of arguments allowed
- Called from (representative examples):
  - [postprocess_sql_command](postprocess_sql_command.md): Main function that processes SQL commands in pgbench

## Notes and Other Information
- The function enforces a maximum number of parameters (MAX_ARGS - 1) to prevent excessive memory usage
- Parameter counting starts at 1 since cmd->argv[0] is reserved for the SQL statement itself
- The function handles consecutive colons by skipping them when they don't form valid variable names
- Memory management is important: the function duplicates the original SQL string and frees memory on errors
- This is part of pgbench's parameter substitution mechanism that allows for parameterized SQL execution

## Simplified Source

```c
static bool parseQuery(Command *cmd)
{
    char *sql, *p;

    cmd->argc = 1;

    // Duplicate SQL string for modification
    p = sql = pg_strdup(cmd->lines.data);

    // Find and replace each :param with $n
    while ((p = strchr(p, ':')) != NULL) {
        char var[13];
        char *name;
        int eaten;

        // Extract variable name from :param
        name = parseVariable(p, &eaten);
        if (name == NULL) {
            // Skip consecutive colons
            while (*p == ':')
                p++;
            continue;
        }

        // Check argument limit (argv[0] is reserved for SQL)
        if (cmd->argc >= MAX_ARGS) {
            pg_log_error("statement has too many arguments (maximum is %d): %s",
                        MAX_ARGS - 1, cmd->lines.data);
            pg_free(name);
            return false;
        }

        // Replace :param with $n in SQL text
        sprintf(var, "$%d", cmd->argc);
        p = replaceVariable(&sql, p, eaten, var);

        // Store parameter name in argv array
        cmd->argv[cmd->argc] = name;
        cmd->argc++;
    }

    // Store modified SQL as first argument
    cmd->argv[0] = sql;
    return true;
}
```