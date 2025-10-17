# accesstype_to_string

## Location
[src/test/modules/test_oat_hooks/test_oat_hooks.c:419-457](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_oat_hooks/test_oat_hooks.c#L419-L457)

## Overview
A utility function that converts ObjectAccessType enumeration values to human-readable string representations, with special handling for parameter access control subId flags.

## Definition
```c
static char *accesstype_to_string(ObjectAccessType access, int subId)
```

## Detailed Description
This function serves as a conversion utility for the test_oat_hooks module, translating PostgreSQL's ObjectAccessType enumeration values into descriptive string representations for logging and auditing purposes. The function provides two levels of information:

1. **Access Type Translation**: Maps each ObjectAccessType enum value to its corresponding string representation (e.g., OAT_POST_CREATE → "create")
2. **SubId Interpretation**: For parameter-related operations, interprets the subId flags to provide additional context about specific permission types (ACL_SET, ACL_ALTER_SYSTEM)

The function uses psprintf() to format the output string, including both the access type name and a hexadecimal representation of the subId value, along with human-readable interpretations of recognized flags.

## Parameters / Member Variables
- `access`: The ObjectAccessType enumeration value to be converted to string
- `subId`: Integer containing bit flags that provide additional context, particularly for ACL operations

## Dependencies
- Functions called/Symbols referenced:
  - [psprintf](../p/psprintf.md)
  - OAT_POST_CREATE
  - OAT_DROP
  - OAT_POST_ALTER
  - OAT_NAMESPACE_SEARCH
  - OAT_FUNCTION_EXECUTE
  - OAT_TRUNCATE
  - ACL_SET
  - ACL_ALTER_SYSTEM
- Called from (representative examples):
  - [REGRESS_object_access_hook_str](../R/REGRESS_object_access_hook_str.md)
  - [REGRESS_object_access_hook](../R/REGRESS_object_access_hook.md)

## Notes and Other Information
- This is a static function used exclusively within the test_oat_hooks module for formatting audit messages
- Returns dynamically allocated strings via psprintf(), requiring proper memory management by callers
- Provides special handling for parameter access control by interpreting ACL_SET and ACL_ALTER_SYSTEM flags in the subId
- The function handles unknown access types by returning "UNRECOGNIZED ObjectAccessType"
- SubId interpretation includes combinations: both flags ("all privileges"), individual flags ("set", "alter system"), or generic hex representation
- Essential for making audit logs human-readable by converting internal enum values to descriptive text
- The hexadecimal subId display helps with debugging by showing the raw flag values
- Used throughout the hook functions to provide consistent string representations for logging purposes

## Simplified Source

```c
static char *accesstype_to_string(ObjectAccessType access, int subId) {
    const char *type;

    // Convert access type enum to string
    switch (access) {
        case OAT_POST_CREATE:
            type = "create";
            break;
        case OAT_DROP:
            type = "drop";
            break;
        case OAT_POST_ALTER:
            type = "alter";
            break;
        case OAT_NAMESPACE_SEARCH:
            type = "namespace search";
            break;
        case OAT_FUNCTION_EXECUTE:
            type = "execute";
            break;
        case OAT_TRUNCATE:
            type = "truncate";
            break;
        default:
            type = "UNRECOGNIZED ObjectAccessType";
    }

    // Handle special subId flag combinations for ACL operations
    if ((subId & ACL_SET) && (subId & ACL_ALTER_SYSTEM)) {
        return psprintf("%s (subId=0x%x, all privileges)", type, subId);
    }
    if (subId & ACL_SET) {
        return psprintf("%s (subId=0x%x, set)", type, subId);
    }
    if (subId & ACL_ALTER_SYSTEM) {
        return psprintf("%s (subId=0x%x, alter system)", type, subId);
    }

    // Default case with hex subId
    return psprintf("%s (subId=0x%x)", type, subId);
}
```