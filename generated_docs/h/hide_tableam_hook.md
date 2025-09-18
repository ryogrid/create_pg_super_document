# hide_tableam_hook

## Location
src/bin/psql/startup.c: 1192 - 1197

## Overview
A validation hook function for the hide_tableam parameter in psql that controls whether table access method information is displayed.

## Definition
```c
static bool hide_tableam_hook(const char *newval)
```

## Detailed Description
This function validates and applies the hide_tableam configuration parameter in psql. It uses the standard ParseVariableBool utility function to parse boolean string values and assign them to the pset.hide_tableam setting. This parameter controls whether table access method (tableam) information is hidden in psql output displays such as \d commands.

## Parameters / Member Variables
- `newval`: The string value to parse as a boolean (accepts standard boolean representations like "true", "false", "on", "off", etc.)

## Dependencies
- Functions called/Symbols referenced:
  - ParseVariableBool (for boolean string parsing and validation)
- Called from (representative examples):
  - EstablishVariableSpace (at src/bin/psql/startup.c:1267)

## Notes and Other Information
- This is a static function defined in src/bin/psql/startup.c
- The function returns true on successful parsing/assignment, false on invalid input
- Uses the standard ParseVariableBool function which handles various boolean string formats
- The setting controls display of table access method information in psql describe commands
- Part of psql's configuration variable system for customizing output formatting
- When enabled (true), table access method details are hidden from describe command output
- Table access methods (tableam) are PostgreSQL's pluggable storage interface system