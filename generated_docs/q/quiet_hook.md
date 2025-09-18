# quiet_hook

## Location
src/bin/psql/startup.c: 887 - 892

## Overview
A hook function used in PostgreSQL's psql client to validate and set the QUIET variable, which controls whether psql should suppress informational output and run in quiet mode.

## Definition


## Detailed Description
The  function serves as a validation and assignment hook for the QUIET psql variable. It is called whenever the user attempts to set the QUIET variable through psql commands like . The function uses the  utility to parse the string value and convert it to a boolean, storing the result in the global  field. This hook ensures that only valid boolean values (like "on", "off", "true", "false", etc.) are accepted for the QUIET setting. When QUIET mode is enabled, psql suppresses various informational messages and runs more silently.

## Parameters / Member Variables
- : A string containing the new value to be assigned to the QUIET variable

## Dependencies
- Functions called/Symbols referenced:
  - ParseVariableBool
- Called from (representative examples):
  - EstablishVariableSpace

## Notes and Other Information
- This is a static function within the psql startup module
- The QUIET variable controls psql's verbosity level
- When QUIET is enabled, psql suppresses informational output like startup messages and command acknowledgments
- The function returns true if the value was successfully parsed and set, false otherwise
- Located in src/bin/psql/startup.c at lines 887-892
- Useful for automated scripts where minimal output is desired