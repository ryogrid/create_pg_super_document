# parse_bool

## Location
src/backend/utils/adt/bool.c: 30 - 35

## Overview
Parses a string value as a boolean, supporting common boolean representations including "true", "false", "yes", "no", "on", "off", "1", and "0".

## Definition


## Detailed Description
The  function is a convenience wrapper around  that attempts to interpret a null-terminated string as a boolean value. It calculates the string length using  and delegates the actual parsing logic to . The function accepts various string representations of boolean values and their unique prefixes, providing flexible boolean parsing for PostgreSQL configuration and data processing.

## Parameters / Member Variables
- : Null-terminated string to be parsed as a boolean value
- : Pointer to a bool variable where the parsed result will be stored (can be NULL if only validation is needed)

## Dependencies
- Functions called/Symbols referenced:
  - [parse_bool_with_len](parse_bool_with_len.md)
  - strlen (standard C library function)
- Called from (representative examples):
  - [parse_one_reloption](parse_one_reloption.md) (reloptions.c:1601)
  - [parse_basebackup_options](parse_basebackup_options.md) (basebackup.c:842)
  - [parse_extension_control_file](parse_extension_control_file.md) (extension.c:566,574,582)
  - [GrantRole](../G/GrantRole.md) (user.c:1500,1506,1512)
  - [ProcessStartupPacket](../P/ProcessStartupPacket.md) (backend_startup.c:738)
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md) (jsonpath_exec.c:1374)
  - parse_and_validate_value (guc.c:3143)

## Notes and Other Information
- Returns true if the string parses successfully as a boolean, false otherwise
- Valid boolean representations include: true, false, yes, no, on, off, 1, 0 (case-insensitive)
- Unique prefixes of the above values are also accepted
- The result parameter can be NULL if only validation (not the actual value) is needed
- This function is commonly used throughout PostgreSQL for parsing configuration options and user input