# assignVariables

## Location
[src/bin/pgbench/pgbench.c:1936-1971](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1936-L1971)

## Overview
Processes SQL strings in pgbench by replacing variable placeholders () with their actual values from the variables store.

## Definition

```c
static char *
assignVariables(Variables *variables, char *sql)
```
## Detailed Description
The  function is responsible for variable substitution in pgbench SQL commands. It scans through a SQL string looking for variable placeholders that start with ':' and replaces them with their corresponding values from the Variables store. The function handles parsing of variable names, retrieval of variable values, and text replacement in the original SQL string. If a variable is not found in the store, the placeholder is left unchanged and processing continues.

## Parameters / Member Variables
- `*variables`: Pointer to the Variables structure containing the variable store with name-value pairs
- `*sql`: The input SQL string that may contain variable placeholders (e.g., ":my_var")
## Dependencies
- Functions called/Symbols referenced:
  - [parseVariable](../p/parseVariable.md) - Parses variable name from placeholder and returns bytes consumed
  - [getVariable](../g/getVariable.md) - Retrieves variable value from the Variables store
  - [replaceVariable](../r/replaceVariable.md) - Performs text replacement of placeholder with actual value
  - [Variables](../V/Variables.md) - Structure type for storing variable name-value pairs
- Called from (representative examples):
  - [sendCommand](../s/sendCommand.md) - Uses assignVariables to prepare SQL before execution

## Notes and Other Information
- [Variable](../V/Variable.md) placeholders must start with ':' followed by a valid variable name
- If a variable is not found in the store, the placeholder remains unchanged
- The function modifies the input SQL string in place through replaceVariable calls
- Memory management is handled properly with free() calls for temporary variable names
- This is a core component of pgbench's variable substitution system, enabling parameterized SQL execution in benchmark scripts

## Simplified Source

```c
static char *
assignVariables(Variables *variables, char *sql)
{
    char *p, *name, *val;

    p = sql;
    while ((p = strchr(p, ':')) != NULL)
    {
        int eaten;

        // Parse variable name from SQL
        name = parseVariable(p, &eaten);
        if (name == NULL)
        {
            // Skip consecutive colons if no valid variable found
            while (*p == ':')
                p++;
            continue;
        }

        // Get variable value from store
        val = getVariable(variables, name);
        free(name);
        if (val == NULL)
        {
            p++;
            continue;
        }

        // Replace placeholder with actual value
        p = replaceVariable(&sql, p, eaten, val);
    }

    return sql;
}
```