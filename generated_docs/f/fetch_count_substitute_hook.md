# fetch_count_substitute_hook

## Location
[src/bin/psql/startup.c:905-912](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L905-L912)

## Overview
A hook function used in PostgreSQL's psql client to provide a default value for the FETCH_COUNT variable when it is unset or null, ensuring the variable always has a valid numeric value.

## Definition


## Detailed Description
The  function serves as a substitute hook for the FETCH_COUNT psql variable. Unlike the boolean validation hooks, this function handles string substitution rather than boolean parsing. When the FETCH_COUNT variable is being set and the provided value is NULL (indicating an unset or cleared variable), this hook provides a default value of "0". This ensures that the FETCH_COUNT variable always has a meaningful value. The FETCH_COUNT variable controls how many rows psql should fetch at a time when executing queries, with 0 meaning fetch all rows at once (the default behavior).

## Parameters / Member Variables
- : A string pointer containing the new value for FETCH_COUNT, or NULL if the variable is being unset

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strdup](../p/pg_strdup.md) (used to allocate memory for the default "0" value)
- Called from (representative examples):
  - [EstablishVariableSpace](../E/EstablishVariableSpace.md)

## Notes and Other Information
- This is a static function within the psql startup module
- Returns the original value if it's not NULL, otherwise returns a duplicated string "0"
- The FETCH_COUNT variable affects query result fetching behavior in psql
- When FETCH_COUNT is set to a positive number, psql fetches that many rows at a time instead of all rows
- A value of 0 means fetch all rows at once (traditional behavior)
- The function returns a newly allocated string when substituting, which must be freed by the caller
- Located in src/bin/psql/startup.c at lines 905-912
- This hook ensures the variable never becomes completely unset, maintaining system stability