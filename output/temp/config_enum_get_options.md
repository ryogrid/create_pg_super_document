# config_enum_get_options

## Overview
Generates a formatted string containing all visible enum option names for a GUC configuration parameter.

## Definition
```c
char *config_enum_get_options(struct config_enum *record, const char *prefix, const char *suffix, const char *separator)
```

## Detailed Description
This function constructs a dynamically allocated string that lists all non-hidden enum options for a given GUC parameter, using the specified separator.

## Parameters / Member Variables
- `record`: Pointer to the config_enum structure
- `prefix`: Optional string to prepend, can be NULL
- `suffix`: Optional string to append, can be NULL
- `separator`: String used to separate option names

## Dependencies
- **Functions called/Symbols referenced**:
  - `config_enum` - Structure type
  - `initStringInfo` - String buffer initialization
- **Called from (representative examples)**:
  - `parse_and_validate_value` - Error message generation

## Notes & Other Information
Returns palloc'd memory that must be freed by the caller.