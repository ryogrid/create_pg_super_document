# histsize_hook

## Location
[src/bin/psql/startup.c:937-942](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L937-L942)

## Overview
A static hook function in psql that validates and processes the HISTSIZE variable when it's being set, ensuring the value is a valid number for controlling the command history size.

## Definition
```c
static bool histsize_hook(const char *newval)
```

## Detailed Description
This function serves as a validation hook for the HISTSIZE psql variable. When a user attempts to set the HISTSIZE variable (which controls the maximum number of commands stored in the command history), this hook function is called to validate that the provided value is a valid numeric value. The function leverages the ParseVariableNum utility to perform the actual parsing and validation of the numeric input.

The HISTSIZE variable in psql determines how many previous commands are kept in memory and potentially saved to the history file. This is similar to the bash HISTSIZE environment variable, allowing users to control their command history retention.

## Parameters / Member Variables
- `newval`: A string containing the new value being assigned to the HISTSIZE variable that needs to be validated and parsed

## Dependencies
- Functions called/Symbols referenced:
  - [ParseVariableNum](../P/ParseVariableNum.md)
- Called from (representative examples):
  - [EstablishVariableSpace](../E/EstablishVariableSpace.md)

## Notes and Other Information
- This is a static function within the psql startup module, making it internal to the psql implementation
- The function returns a boolean indicating whether the parsing and validation was successful
- The parsed value is stored in pset.histsize if validation succeeds
- This hook works in conjunction with histsize_substitute_hook which provides a default value of "500"
- This hook is part of psql's variable management system that ensures type safety and validation for configuration variables
- The validated history size controls both the in-memory command history and potentially the saved history file size