# singleline_hook

## Location
src/bin/psql/startup.c: 893 - 898

## Overview
A hook function used in PostgreSQL's psql client to validate and set the SINGLELINE variable, which controls whether psql should treat each line as a separate command instead of using multi-line SQL statement parsing.

## Definition


## Detailed Description
The  function serves as a validation and assignment hook for the SINGLELINE psql variable. It is called whenever the user attempts to set the SINGLELINE variable through psql commands like . The function uses the  utility to parse the string value and convert it to a boolean, storing the result in the global  field. This hook ensures that only valid boolean values are accepted for the SINGLELINE setting. When SINGLELINE mode is enabled, psql processes each line as a complete command rather than allowing multi-line SQL statements, which can be useful for certain scripting scenarios or when processing line-oriented input.

## Parameters / Member Variables
- : A string containing the new value to be assigned to the SINGLELINE variable

## Dependencies
- Functions called/Symbols referenced:
  - [ParseVariableBool](../P/ParseVariableBool.md)
- Called from (representative examples):
  - [EstablishVariableSpace](../E/EstablishVariableSpace.md)

## Notes and Other Information
- This is a static function within the psql startup module
- The SINGLELINE variable affects how psql parses SQL input
- When SINGLELINE is enabled, each line is treated as a complete statement, eliminating the need for semicolon terminators
- This mode can be useful for processing simple SQL commands from scripts or other automated sources
- The function returns true if the value was successfully parsed and set, false otherwise
- Located in src/bin/psql/startup.c at lines 893-898
- Differs from normal psql behavior where SQL statements can span multiple lines