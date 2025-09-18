# REGRESS_object_access_hook_str

## Location
src/test/modules/test_oat_hooks/test_oat_hooks.c: 277 - 324

## Overview
A test hook function that intercepts object access operations for regression testing, providing auditing capabilities and enforcing permission restrictions on parameter configuration operations.

## Definition
```c
static void REGRESS_object_access_hook_str(ObjectAccessType access, Oid classId, const char *objName, int subId, void *arg)
```

## Detailed Description
This function serves as a custom object access hook specifically designed for PostgreSQL regression testing. It implements the object_access_hook_str_type interface to monitor and control access to database objects identified by string names rather than OIDs. The function performs three main operations:

1. **Auditing**: Records both attempted and successful object access operations using the audit_attempt() and audit_success() functions
2. **Hook Chaining**: Calls the next hook in the chain if one exists (next_object_access_hook_str)
3. **Permission Enforcement**: For OAT_POST_ALTER operations, it enforces custom permission restrictions on parameter configuration based on ACL flags

The function specifically handles parameter access control by checking various ACL permission combinations (ACL_SET, ACL_ALTER_SYSTEM) and can deny operations to non-superusers when certain regression test flags are enabled.

## Parameters / Member Variables
- `access`: The type of object access operation being performed (ObjectAccessType)
- `classId`: The system catalog OID identifying the object class
- `objName`: String name of the object being accessed
- `subId`: Subtype identifier containing ACL permission flags for parameter operations
- `arg`: Additional argument data passed through the hook chain

## Dependencies
- Functions called/Symbols referenced:
  - [audit_attempt](../a/audit_attempt.md)
  - [accesstype_to_string](../a/accesstype_to_string.md)
  - [audit_success](../a/audit_success.md)
  - superuser_arg
  - [GetUserId](../G/GetUserId.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - elog
  - [pstrdup](../p/pstrdup.md)
- Called from (representative examples):
  - [_PG_init](../P/_PG_init.md) (hook installation)

## Notes and Other Information
- This is a static function used exclusively for regression testing in the test_oat_hooks module
- The function handles hook chaining by calling next_object_access_hook_str if it exists
- Permission enforcement is controlled by global regression test flags (REGRESS_deny_set_variable, REGRESS_deny_alter_system)
- Only processes OAT_POST_ALTER access types for permission checking; other access types pass through without restriction
- Uses PostgreSQL's error reporting mechanism to deny unauthorized parameter configuration attempts
- Part of the object access hook testing framework for validating security and auditing functionality