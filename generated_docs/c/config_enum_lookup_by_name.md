# config_enum_lookup_by_name

## Location
src/backend/utils/misc/guc.c: 3048 - 3073

## Overview
Performs case-insensitive lookup of an enum option by name and returns its corresponding integer value.

## Definition


## Detailed Description
This function searches through the options array of a PostgreSQL configuration enum to find a matching name (case-insensitive comparison). It iterates through all available enum entries and uses pg_strcasecmp() for string comparison. When a match is found, it sets the corresponding integer value through the retval parameter and returns true. If no match is found, it sets retval to 0 and returns false.

The function is essential for converting human-readable enum option names (like "on", "off", "auto") into their internal integer representations used by PostgreSQL's configuration system.

## Parameters / Member Variables
- `record`: Pointer to a config_enum structure containing the enum definition and options array
- `value`: String name of the enum option to look up (case-insensitive)
- `retval`: Output parameter that receives the integer value corresponding to the found enum option, or 0 if not found

## Dependencies
- Functions called/Symbols referenced:
  - config_enum (struct type)
  - config_enum_entry (struct type)
  - pg_strcasecmp (for case-insensitive string comparison)
- Called from (representative examples):
  - parse_and_validate_value

## Notes and Other Information
- The function performs case-insensitive matching using pg_strcasecmp()
- Returns false and sets retval to 0 when no matching enum option is found
- Essential component of PostgreSQL's configuration parameter validation system
- Used internally by the GUC (Grand Unified Configuration) system for enum-type parameters