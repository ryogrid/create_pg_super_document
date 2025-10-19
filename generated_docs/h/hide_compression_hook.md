# hide_compression_hook

## Location
[src/bin/psql/startup.c:1185-1191](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L1185-L1191)

## Overview
A validation hook function for the hide_compression parameter in psql that controls whether TOAST compression information is displayed.

## Definition
```c
static bool hide_compression_hook(const char *newval)
```

## Detailed Description
This function validates and applies the hide_compression configuration parameter in psql. It uses the standard ParseVariableBool utility function to parse boolean string values and assign them to the pset.hide_compression setting. This parameter controls whether TOAST (The Oversized-Attribute Storage Technique) compression details are hidden in psql output displays.

## Parameters / Member Variables
- `newval`: The string value to parse as a boolean (accepts standard boolean representations like "true", "false", "on", "off", etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [ParseVariableBool](../P/ParseVariableBool.md) (for boolean string parsing and validation)
- Called from (representative examples):
  - [EstablishVariableSpace](../E/EstablishVariableSpace.md) (at src/bin/psql/startup.c:1264)

## Notes and Other Information
- This is a static function defined in src/bin/psql/startup.c
- The function returns true on successful parsing/assignment, false on invalid input
- Uses the standard ParseVariableBool function which handles various boolean string formats
- The setting controls display of TOAST compression information in psql query results
- Part of psql's configuration variable system for customizing output formatting
- When enabled (true), compression details for TOAST values are hidden from display

## Simplified Source

```c
static bool hide_compression_hook(const char *newval) {
    // Parse boolean value and store in pset.hide_compression
    return ParseVariableBool(newval, "HIDE_TOAST_COMPRESSION", &pset.hide_compression);
}
```