# fetch_count_hook

## Location
[src/bin/psql/startup.c:913-918](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L913-L918)

## Overview
A static hook function in psql that validates and processes the FETCH_COUNT variable when it's being set, ensuring the value is a valid number for controlling how many rows to fetch at once.

## Definition

```c
static bool
fetch_count_hook(const char *newval)
```
## Detailed Description
This function serves as a validation hook for the FETCH_COUNT psql variable. When a user attempts to set the FETCH_COUNT variable (which controls how many rows are fetched at once in psql), this hook function is called to validate that the provided value is a valid numeric value. The function leverages the ParseVariableNum utility to perform the actual parsing and validation of the numeric input.

The FETCH_COUNT variable in psql determines how many rows should be retrieved and displayed at once when executing queries. Setting it to 0 means all rows are fetched, while positive values limit the number of rows fetched per batch.

## Parameters / Member Variables
- `*newval`: A string containing the new value being assigned to the FETCH_COUNT variable that needs to be validated and parsed
## Dependencies
- Functions called/Symbols referenced:
  - [ParseVariableNum](../P/ParseVariableNum.md)
- Called from (representative examples):
  - [EstablishVariableSpace](../E/EstablishVariableSpace.md)

## Notes and Other Information
- This is a static function within the psql startup module, making it internal to the psql implementation
- The function returns a boolean indicating whether the parsing and validation was successful
- The parsed value is stored in pset.fetch_count if validation succeeds
- This hook is part of psql's variable management system that ensures type safety and validation for configuration variables

## Simplified Source

```c
static bool fetch_count_hook(const char *newval) {
    // Parse and validate numeric value, then set fetch count
    return ParseVariableNum(newval, "FETCH_COUNT", &pset.fetch_count);
}
```

This hook function:
1. Validates the new string value as a number
2. Sets the global FETCH_COUNT value if valid
3. Controls how many rows psql fetches at once (0 = all rows)
4. Returns true on success, false on invalid input