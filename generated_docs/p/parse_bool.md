# parse_bool

## Location
[src/backend/utils/adt/bool.c:30-35](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/bool.c#L30-L35)

## Overview
Parses a string value as a boolean, supporting common boolean representations including "true", "false", "yes", "no", "on", "off", "1", and "0".

## Definition

```c
bool
parse_bool(const char *value, bool *result)
```
## Detailed Description
The  function is a convenience wrapper around  that attempts to interpret a null-terminated string as a boolean value. It calculates the string length using  and delegates the actual parsing logic to . The function accepts various string representations of boolean values and their unique prefixes, providing flexible boolean parsing for PostgreSQL configuration and data processing.

## Parameters / Member Variables
- `*value`: Null-terminated string to be parsed as a boolean value
- `*result`: Pointer to a bool variable where the parsed result will be stored (can be NULL if only validation is needed)
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
  - [parse_and_validate_value](parse_and_validate_value.md) (guc.c:3143)

## Notes and Other Information
- Returns true if the string parses successfully as a boolean, false otherwise
- Valid boolean representations include: true, false, yes, no, on, off, 1, 0 (case-insensitive)
- [Unique](../U/Unique.md) prefixes of the above values are also accepted
- The result parameter can be NULL if only validation (not the actual value) is needed
- This function is commonly used throughout PostgreSQL for parsing configuration options and user input

## Simplified Source

```c
// Simplified version of parse_bool
bool parse_bool(const char *value, bool *result) {
    size_t len = strlen(value);

    // Check the most common boolean representations
    switch (*value) {
        case 't': case 'T':
            // "true" and variants
            if (pg_strncasecmp(value, "true", len) == 0) {
                if (result) *result = true;
                return true;
            }
            break;

        case 'f': case 'F':
            // "false" and variants
            if (pg_strncasecmp(value, "false", len) == 0) {
                if (result) *result = false;
                return true;
            }
            break;

        case 'y': case 'Y':
            // "yes" and variants
            if (pg_strncasecmp(value, "yes", len) == 0) {
                if (result) *result = true;
                return true;
            }
            break;

        case 'n': case 'N':
            // "no" and variants
            if (pg_strncasecmp(value, "no", len) == 0) {
                if (result) *result = false;
                return true;
            }
            break;

        case 'o': case 'O':
            // "on" or "off" - requires at least 2 characters for disambiguation
            if (pg_strncasecmp(value, "on", max(len, 2)) == 0) {
                if (result) *result = true;
                return true;
            }
            if (pg_strncasecmp(value, "off", max(len, 2)) == 0) {
                if (result) *result = false;
                return true;
            }
            break;

        case '1':
            // Single digit "1"
            if (len == 1) {
                if (result) *result = true;
                return true;
            }
            break;

        case '0':
            // Single digit "0"
            if (len == 1) {
                if (result) *result = false;
                return true;
            }
            break;
    }

    // No valid boolean representation found
    return false;
}
```

Key simplifications made:
- Combined the wrapper function with the actual parsing logic for clarity
- Consolidated the case-insensitive string matching logic
- Simplified the 'o' case handling while preserving the disambiguation requirement
- Removed redundant error suppression code
- Added clear comments for each boolean representation type
- Maintained the exact same functionality and return behavior