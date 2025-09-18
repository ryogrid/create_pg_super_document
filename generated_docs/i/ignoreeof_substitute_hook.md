# ignoreeof_substitute_hook

## Location
src/bin/psql/startup.c: 943 - 963

## Overview
A static substitute hook function in psql that provides default values for the IGNOREEOF variable, implementing bash-like behavior for handling consecutive EOF characters.

## Definition
```c
static char *ignoreeof_substitute_hook(char *newval)
```

## Detailed Description
This function serves as a substitute hook for the IGNOREEOF psql variable, which controls how many consecutive EOF characters must be typed before psql exits. The function implements behavior similar to bash's IGNOREEOF variable, with some psql-specific modifications.

The function handles three scenarios:
1. If no value is provided (newval is NULL), it defaults to "0", meaning EOF will immediately cause psql to exit
2. If a value is provided but is not a valid integer, it defaults to "10", requiring 10 consecutive EOF characters before exit
3. If a valid integer value is provided, it returns that value unchanged

This differs slightly from bash behavior - while bash allows non-numeric values to default to 10, psql insists that the stored value must be a valid integer, providing "10" as a substitute for invalid numeric inputs.

## Parameters / Member Variables
- `newval`: A string containing the current value of the IGNOREEOF variable, or NULL if no value has been set

## Dependencies
- Functions called/Symbols referenced:
  - ParseVariableNum
  - pg_strdup (implicitly called for string duplication)
- Called from (representative examples):
  - EstablishVariableSpace

## Notes and Other Information
- This is a static function within the psql startup module, making it internal to the psql implementation
- The function returns a char* that may be a newly allocated string when providing default values
- Default behavior: NULL → "0" (immediate exit), invalid number → "10" (require 10 EOFs)
- The function uses a dummy integer variable for validation purposes when checking if the input is a valid number
- This hook mimics bash IGNOREEOF behavior but enforces stricter numeric validation
- The returned string becomes the actual value used for the IGNOREEOF variable
- Memory allocated by pg_strdup must eventually be freed by the calling code