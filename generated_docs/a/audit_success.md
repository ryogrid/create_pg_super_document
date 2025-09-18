# audit_success

## Location
src/test/modules/test_oat_hooks/test_oat_hooks.c: 265 - 270

## Overview
A static wrapper function in the test_oat_hooks module that logs successful completion events for testing object access control hooks.

## Definition
```c
static void audit_success(const char *hook, char *action, char *objName)
```

## Detailed Description
audit_success is a semantic wrapper function that provides a clear interface for logging successful completion of access control operations in PostgreSQL's object access testing framework. It calls emit_audit_message with a fixed "finished" type parameter, indicating that a security hook evaluation has completed successfully and the requested operation was allowed to proceed. This function works in conjunction with audit_attempt() and audit_failure() to provide a complete audit trail of access control decisions during testing.

## Parameters / Member Variables
- `hook`: The name of the hook that completed successfully (e.g., "object_access_hook_str", "exec_check_perms")
- `action`: A dynamically allocated string describing the specific action that was completed (passed through to emit_audit_message for cleanup)
- `objName`: A dynamically allocated string containing the object name that was successfully accessed (passed through to emit_audit_message for cleanup, may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [emit_audit_message](../e/emit_audit_message.md)() (core audit message emission function)
- Called from (representative examples):
  - [REGRESS_object_access_hook_str](../R/REGRESS_object_access_hook_str.md)() (for string-based object access hooks)
  - [REGRESS_object_access_hook](../R/REGRESS_object_access_hook.md)() (for OID-based object access hooks)
  - [REGRESS_exec_check_perms](../R/REGRESS_exec_check_perms.md)() (for executor permission checks)
  - [REGRESS_utility_command](../R/REGRESS_utility_command.md)() (for utility command hooks)

## Notes and Other Information
- Part of the test_oat_hooks module for mandatory access control (MAC) testing
- Provides semantic clarity by using "finished" as the message type to indicate successful completion
- Located at src/test/modules/test_oat_hooks/test_oat_hooks.c:265-270
- Complements audit_attempt() and audit_failure() for comprehensive access control audit logging
- Memory management for action and objName parameters is delegated to emit_audit_message()
- Used to verify that legitimate access requests are properly allowed by the security framework