# prompt2_hook

## Location
[src/bin/psql/startup.c:1105-1111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L1105-L1111)

## Overview
A variable hook function that updates the PROMPT2 variable in psql, which controls the continuation prompt displayed when a multi-line command is being entered.

## Definition

```c
static bool
prompt2_hook(const char *newval)
```
## Detailed Description
This hook function is responsible for updating the psql prompt2 setting when the PROMPT2 variable is modified. It is called whenever the user sets or changes the PROMPT2 variable in psql. The function simply copies the new value to the pset.prompt2 field, which is used internally by psql to display the continuation prompt. If a NULL value is passed, it defaults to an empty string.

## Parameters / Member Variables
- : The new value for the PROMPT2 variable as a C string. If NULL, defaults to an empty string.

## Dependencies
- Functions called/Symbols referenced:
  - pset.prompt2 (global variable assignment)
- Called from (representative examples):
  - SetVariableHooks registration in EstablishVariableSpace

## Notes and Other Information
- This is one of several prompt hook functions (prompt1_hook, prompt2_hook, prompt3_hook) that manage different prompt types in psql
- The function always returns true, indicating successful processing
- PROMPT2 is typically used for continuation lines when entering multi-line SQL commands
- Located in src/bin/psql/startup.c:1105