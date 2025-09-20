# prompt3_hook

## Location
[src/bin/psql/startup.c:1112-1118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L1112-L1118)

## Overview
A variable hook function that updates the PROMPT3 variable in psql, which controls the prompt displayed when the SQL statement is incomplete due to missing right parentheses, quotes, or other syntax elements.

## Definition

```c
static bool
prompt3_hook(const char *newval)
```
## Detailed Description
This hook function is responsible for updating the psql prompt3 setting when the PROMPT3 variable is modified. It is called whenever the user sets or changes the PROMPT3 variable in psql. The function simply copies the new value to the pset.prompt3 field, which is used internally by psql to display the prompt when the SQL statement appears to be incomplete. If a NULL value is passed, it defaults to an empty string.

## Parameters / Member Variables
- : The new value for the PROMPT3 variable as a C string. If NULL, defaults to an empty string.

## Dependencies
- Functions called/Symbols referenced:
  - pset.prompt3 (global variable assignment)
- Called from (representative examples):
  - SetVariableHooks registration in EstablishVariableSpace

## Notes and Other Information
- This is one of several prompt hook functions (prompt1_hook, prompt2_hook, prompt3_hook) that manage different prompt types in psql
- The function always returns true, indicating successful processing
- PROMPT3 is specifically used when psql detects an incomplete SQL statement that needs additional input to complete
- Located in src/bin/psql/startup.c:1112