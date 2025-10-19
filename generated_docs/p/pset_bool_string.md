# pset_bool_string

## Location
[src/bin/psql/command.c:5148-5154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L5148-L5154)

## Overview
A simple utility function that converts a boolean value to its string representation used in PostgreSQL psql settings.

## Definition

```c
static const char *
pset_bool_string(bool val)
```
## Detailed Description
The pset_bool_string function provides a standardized way to convert boolean values to their string representations within psql. It returns "on" for true values and "off" for false values, following PostgreSQL's conventional boolean string format used throughout the psql interface for various settings and options.

This is a static helper function that ensures consistency in boolean string representation across psql's setting system.

## Parameters / Member Variables
- `val`: Boolean value to be converted to string representation
## Dependencies
- Functions called/Symbols referenced:
  - None (uses only basic C constructs)
- Called from (representative examples):
  - [pset_value_string](pset_value_string.md) (multiple calls)

## Notes and Other Information
- Returns constant string literals, so no memory management is required
- Uses PostgreSQL's standard "on"/"off" convention rather than "true"/"false"
- Static function scope limits its usage to within command.c
- Commonly used by pset_value_string for formatting boolean settings in psql output
- Simple ternary operator implementation for maximum efficiency

## Simplified Source

```c
static const char *
pset_bool_string(bool val)
{
    // Convert boolean to PostgreSQL standard string format
    return val ? "on" : "off";
}
```