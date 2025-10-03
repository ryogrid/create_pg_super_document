# ReplicationSlotValidateName

## Location
[src/backend/replication/slot.c:252-308](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L252-L308)

## Overview
Validates replication slot names according to PostgreSQL naming rules and reports appropriate errors if validation fails.

## Definition
```c
bool ReplicationSlotValidateName(const char *name, int elevel)
```

## Detailed Description
This function validates replication slot names to ensure they conform to PostgreSQL's naming requirements. Valid slot names must consist only of lowercase letters (a-z), digits (0-9), and underscores (_), with a length between 1 and NAMEDATALEN-1 characters. This restriction ensures the slot name can be safely used as a directory name on all supported operating systems.

The function performs three validation checks:
1. Name is not empty (length > 0)
2. Name is not too long (length < NAMEDATALEN)  
3. All characters are valid (lowercase letters, digits, underscores only)

If validation fails, appropriate error messages are reported using the specified error level. The function can be used with different error levels to control whether validation failures should terminate processing or just log warnings.

## Parameters / Member Variables
- `*name`: The replication slot name string to validate
- `elevel`: Error level for reporting validation failures (e.g., ERROR, WARNING, LOG)
## Dependencies
- Functions called/Symbols referenced:
  - strlen (implicit)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [errhint](../e/errhint.md)
  - NAMEDATALEN (constant)
  - ERRCODE_INVALID_NAME (constant)
  - ERRCODE_NAME_TOO_LONG (constant)
- Called from (representative examples):
  - [ReplicationSlotCreate](ReplicationSlotCreate.md)
  - [check_primary_slot_name](../c/check_primary_slot_name.md)
  - [parse_subscription_options](../p/parse_subscription_options.md)
  - [StartupReorderBuffer](../S/StartupReorderBuffer.md)

## Notes and Other Information
- Returns true if the name is valid, false if invalid
- The character validation ensures filesystem compatibility across different operating systems
- Uses PostgreSQL's standard error reporting mechanism with appropriate error codes
- NAMEDATALEN is PostgreSQL's maximum identifier length constant
- The validation is strict: uppercase letters are not allowed, only lowercase
- Provides helpful error hints to guide users on proper naming conventions

## Simplified Source

```c
// Simplified version of ReplicationSlotValidateName
bool ReplicationSlotValidateName(const char *name, int elevel) {
    // Check 1: Name cannot be empty
    if (strlen(name) == 0) {
        ereport(elevel, (errcode(ERRCODE_INVALID_NAME),
                        errmsg("replication slot name \"%s\" is too short", name)));
        return false;
    }

    // Check 2: Name cannot exceed maximum length
    if (strlen(name) >= NAMEDATALEN) {
        ereport(elevel, (errcode(ERRCODE_NAME_TOO_LONG),
                        errmsg("replication slot name \"%s\" is too long", name)));
        return false;
    }

    // Check 3: Validate each character (only a-z, 0-9, underscore allowed)
    for (const char *cp = name; *cp; cp++) {
        if (!((*cp >= 'a' && *cp <= 'z') ||
              (*cp >= '0' && *cp <= '9') ||
              (*cp == '_'))) {
            ereport(elevel, (errcode(ERRCODE_INVALID_NAME),
                            errmsg("replication slot name \"%s\" contains invalid character", name),
                            errhint("Replication slot names may only contain lower case letters, numbers, and the underscore character.")));
            return false;
        }
    }

    return true;
}
```

Key simplifications made:
- Consolidated character validation logic into clearer conditional structure
- Added descriptive comments for each validation step
- Improved variable declaration readability (moved const char *cp declaration inline)
- Preserved all essential validation logic and error reporting
- Maintained original function signature and return behavior