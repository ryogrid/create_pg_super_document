# REGRESS_object_access_hook

## Location
[src/test/modules/test_oat_hooks/test_oat_hooks.c:325-347](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_oat_hooks/test_oat_hooks.c#L325-L347)

## Overview
A test hook function that intercepts object access operations for regression testing, providing auditing capabilities and enforcing blanket permission restrictions on object access operations.

## Definition
```c
static void REGRESS_object_access_hook(ObjectAccessType access, Oid classId, Oid objectId, int subId, void *arg)
```

## Detailed Description
This function serves as a custom object access hook specifically designed for PostgreSQL regression testing. It implements the object_access_hook_type interface to monitor and control access to database objects identified by OIDs. The function performs three main operations:

1. **Auditing**: Records both attempted and successful object access operations using audit_attempt() and audit_success() functions
2. **Permission Enforcement**: Can deny all object access operations to non-superusers when the REGRESS_deny_object_access flag is enabled
3. **Hook Chaining**: Forwards the call to the next hook in the chain if one exists (next_object_access_hook)

Unlike REGRESS_object_access_hook_str which handles specific parameter operations, this function provides a more general object access control mechanism that can block any object access operation based on the regression test configuration.

## Parameters / Member Variables
- `access`: The type of object access operation being performed (ObjectAccessType)
- `classId`: The system catalog OID identifying the object class
- `objectId`: The OID of the specific object being accessed
- `subId`: Subtype identifier providing additional context for the operation
- `arg`: Additional argument data passed through the hook chain

## Dependencies
- Functions called/Symbols referenced:
  - [audit_attempt](../a/audit_attempt.md)
  - [accesstype_to_string](../a/accesstype_to_string.md)
  - [accesstype_arg_to_string](../a/accesstype_arg_to_string.md)
  - [superuser_arg](../s/superuser_arg.md)
  - [GetUserId](../G/GetUserId.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [audit_success](../a/audit_success.md)
- Called from (representative examples):
  - [_PG_init](../P/_PG_init.md) (hook installation)

## Notes and Other Information
- This is a static function used exclusively for regression testing in the test_oat_hooks module
- Provides a simple on/off mechanism for object access control via the REGRESS_deny_object_access flag
- Unlike the string-based hook variant, this function operates on object OIDs rather than string names
- The function maintains hook chaining by calling next_object_access_hook if it exists
- Uses accesstype_arg_to_string() to convert access-specific argument data to human-readable strings for auditing
- Part of the comprehensive object access hook testing framework for validating PostgreSQL's security infrastructure
- The hook is installed during module initialization and remains active throughout the session