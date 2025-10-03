# singlestep_hook

## Location
[src/bin/psql/startup.c:899-904](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L899-L904)

## Overview
A hook function used in PostgreSQL's psql client to validate and set the SINGLESTEP variable, which controls whether psql should prompt the user before executing each command in interactive single-step debugging mode.

## Definition

```c
static bool
singlestep_hook(const char *newval)
```
## Detailed Description
The  function serves as a validation and assignment hook for the SINGLESTEP psql variable. It is called whenever the user attempts to set the SINGLESTEP variable through psql commands like . The function uses the  utility to parse the string value and convert it to a boolean, storing the result in the global  field. This hook ensures that only valid boolean values are accepted for the SINGLESTEP setting. When SINGLESTEP mode is enabled, psql will prompt the user before executing each SQL statement, providing an interactive debugging and review capability useful for carefully stepping through scripts or complex command sequences.

## Parameters / Member Variables
- `*newval`: A string containing the new value to be assigned to the SINGLESTEP variable
## Dependencies
- Functions called/Symbols referenced:
  - [ParseVariableBool](../P/ParseVariableBool.md)
- Called from (representative examples):
  - [EstablishVariableSpace](../E/EstablishVariableSpace.md)

## Notes and Other Information
- This is a static function within the psql startup module
- The SINGLESTEP variable enables interactive step-by-step execution mode
- When SINGLESTEP is enabled, psql pauses before executing each command and waits for user confirmation
- This feature is particularly useful for debugging SQL scripts or when you want to review each command before execution
- The function returns true if the value was successfully parsed and set, false otherwise
- Located in src/bin/psql/startup.c at lines 899-904
- Provides a debugging mechanism similar to single-step execution in debuggers