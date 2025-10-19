# histsize_substitute_hook

## Location
[src/bin/psql/startup.c:929-936](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L929-L936)

## Overview
A static substitute hook function in psql that provides a default value of "500" for the HISTSIZE variable when no value is explicitly set.

## Definition
```c
static char *histsize_substitute_hook(char *newval)
```

## Detailed Description
This function serves as a substitute hook for the HISTSIZE psql variable, which controls the maximum number of commands stored in the command history. The substitute hook is called when no explicit value has been provided for the variable, allowing the system to supply a sensible default value.

When the HISTSIZE variable is not set (newval is NULL), this function allocates memory for and returns a string containing "500", establishing 500 as the default history size. If a value is already provided (newval is not NULL), the function simply returns the existing value unchanged.

The HISTSIZE variable in psql controls how many previous commands are kept in memory and potentially saved to the history file, similar to the bash HISTSIZE environment variable.

## Parameters / Member Variables
- `newval`: A string containing the current value of the HISTSIZE variable, or NULL if no value has been set

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strdup](../p/pg_strdup.md) (implicitly called to duplicate the "500" string)
- Called from (representative examples):
  - [EstablishVariableSpace](../E/EstablishVariableSpace.md)

## Notes and Other Information
- This is a static function within the psql startup module, making it internal to the psql implementation
- The function returns a char* that may be a newly allocated string (when providing the default value)
- The default history size is set to 500 commands
- This hook is part of psql's variable management system that allows variables to have default values
- The returned string becomes the actual value used for the HISTSIZE variable
- When returning the default value, memory is allocated using pg_strdup which must eventually be freed

## Simplified Source

```c
static char *histsize_substitute_hook(char *newval) {
    // If value is NULL, provide default history size of "500"
    if (newval == NULL)
        newval = pg_strdup("500");
    return newval;
}
```

This substitute hook:
1. Checks if the HISTSIZE value is NULL (unset)
2. If NULL, allocates and returns "500" as the default history size
3. Otherwise, returns the original value unchanged
4. Ensures HISTSIZE always has a valid numeric value for command history limit