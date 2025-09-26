# check_createrole_self_grant

## Location
[src/backend/commands/user.c:2515-2567](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/user.c#L2515-L2567)

## Overview
GUC check hook function that validates and parses the createrole_self_grant configuration parameter, ensuring it contains only valid role option keywords.

## Definition
```c
bool check_createrole_self_grant(char **newval, void **extra, GucSource source)
```

## Detailed Description
This function serves as a validation hook for the createrole_self_grant GUC (Grand Unified Configuration) parameter. It parses a comma-separated list of role options that determine what privileges a user with CREATEROLE can automatically grant to themselves when creating roles. The function:

1. **Parsing**: Splits the input string into individual tokens using SplitIdentifierString
2. **Validation**: Checks each token against allowed keywords ("SET", "INHERIT")
3. **Conversion**: Converts valid keywords into their corresponding bitmask values
4. **Memory Management**: Allocates memory for the parsed result and stores it in the extra parameter
5. **Error Handling**: Provides detailed error messages for invalid syntax or unrecognized keywords

The function ensures that only valid role options are accepted for the configuration parameter.

## Parameters / Member Variables
- `newval`: Pointer to the new string value being set for the GUC parameter
- `extra`: Pointer to store additional parsed data (the bitmask of options)
- `source`: GucSource indicating where the configuration value is coming from

## Dependencies
- Functions called/Symbols referenced:
  - pstrdup
  - SplitIdentifierString
  - GUC_check_errdetail
  - pfree
  - list_free
  - pg_strcasecmp
  - lfirst
  - guc_malloc
- Types used:
  - GucSource
  - List
  - ListCell
- Constants:
  - GRANT_ROLE_SPECIFIED_SET
  - GRANT_ROLE_SPECIFIED_INHERIT
  - LOG
- Called from:
  - PostgreSQL GUC system (referenced in user.h)

## Notes and Other Information
- This is a GUC check hook function, part of PostgreSQL's configuration parameter validation system
- Currently supports only "SET" and "INHERIT" keywords, rejecting any other values
- The function performs case-insensitive comparison of keywords using pg_strcasecmp
- Memory allocated via guc_malloc is automatically managed by the GUC system
- Returns false on any validation error, preventing the configuration change
- The parsed bitmask is stored in the extra parameter for use by the GUC system
- Provides detailed error messages to help users correct invalid configurations
- Part of PostgreSQL's role-based access control configuration infrastructure
- The function ensures that the createrole_self_grant parameter maintains valid syntax and values