# accesstype_to_string

## Location
src/test/modules/test_oat_hooks/test_oat_hooks.c: 419 - 457

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
  - psprintf
  - OAT_POST_CREATE
  - OAT_DROP
  - OAT_POST_ALTER
  - OAT_NAMESPACE_SEARCH
  - OAT_FUNCTION_EXECUTE
  - OAT_TRUNCATE
  - ACL_SET
  - ACL_ALTER_SYSTEM
- Called from (representative examples):
  - REGRESS_object_access_hook_str
  - REGRESS_object_access_hook

## Notes and Other Information
- This is a static function used exclusively within the test_oat_hooks module for formatting audit messages
- Returns dynamically allocated strings via psprintf(), requiring proper memory management by callers
- Provides special handling for parameter access control by interpreting ACL_SET and ACL_ALTER_SYSTEM flags in the subId
- The function handles unknown access types by returning "UNRECOGNIZED ObjectAccessType"
- SubId interpretation includes combinations: both flags ("all privileges"), individual flags ("set", "alter system"), or generic hex representation
- Essential for making audit logs human-readable by converting internal enum values to descriptive text
- The hexadecimal subId display helps with debugging by showing the raw flag values
- Used throughout the hook functions to provide consistent string representations for logging purposes