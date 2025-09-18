# ignoreeof_hook

## Location
[src/bin/psql/startup.c:964-969](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L964-L969)

## Overview
A validation hook function for the IGNOREEOF psql variable that parses and validates integer values for controlling the number of EOF characters needed to exit psql.

## Definition


## Detailed Description
The  function is a psql variable hook that validates new values assigned to the IGNOREEOF variable. It parses string input to ensure it represents a valid integer and stores the parsed value in . The IGNOREEOF variable controls how many consecutive EOF (Ctrl-D) characters are required before psql will exit - a value of 0 means exit immediately on EOF, while positive values require that many consecutive EOF inputs.

This hook is part of psql's variable system that provides validation and processing for configuration variables. When a user sets the IGNOREEOF variable (e.g., via ), this hook function is called to validate the input.

## Parameters / Member Variables
- : The string value being assigned to the IGNOREEOF variable that needs to be validated and parsed

## Dependencies
- Functions called/Symbols referenced:
  - [ParseVariableNum](../P/ParseVariableNum.md)
- Called from (representative examples):
  - [EstablishVariableSpace](../E/EstablishVariableSpace.md) (registers the hook)

## Notes and Other Information
- This is a static function within src/bin/psql/startup.c, used internally by psql's variable system
- The function returns true if the value is successfully parsed as an integer, false otherwise
- Invalid values will not modify  and an error message will be displayed to the user
- The hook is registered in EstablishVariableSpace() alongside a substitute hook for the IGNOREEOF variable
- Part of psql's comprehensive variable hook system that ensures type safety and validation for configuration settings