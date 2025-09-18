# conninfo_find

## Location
src/interfaces/libpq/fe-connect.c: 6928 - 6945

## Overview
A static utility function that searches for a specific connection option by keyword in the PQconninfoOption array.

## Definition


## Detailed Description
This function performs a linear search through the connOptions array to locate a connection option that matches the specified keyword. It iterates through the array until it either finds a matching option or reaches the end of the array (indicated by a NULL keyword field). The search is case-sensitive and uses string comparison to match keywords exactly.

The function is a fundamental building block for PostgreSQL's connection option management system, providing the lookup mechanism needed to access and modify specific connection parameters.

## Parameters / Member Variables
- `connOptions`: Array of PQconninfoOption structures to search through
- `keyword`: The connection option name to search for (case-sensitive)

## Dependencies
- Functions called/Symbols referenced:
  - strcmp
- Called from (representative examples):
  - internalPQconninfoOption
  - conninfo_getval
  - conninfo_storeval

## Notes and Other Information
- This is a static internal function not exposed in the public libpq API
- Performs linear search through the options array - assumes array is NULL-terminated
- Returns NULL if the keyword is not found in the array
- Case-sensitive keyword matching using strcmp
- Simple and efficient implementation for the typical small number of connection options
- Essential utility function used throughout the connection option management code