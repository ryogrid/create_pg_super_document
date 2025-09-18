# audit_attempt

## Location
src/test/modules/test_oat_hooks/test_oat_hooks.c: 259 - 264

## Overview
A static wrapper function in the test_oat_hooks module that logs access attempt events for testing object access control hooks.

## Definition
```c
static void audit_attempt(const char *hook, char *action, char *objName)
```

## Detailed Description
audit_attempt is a simple wrapper function that provides a semantic interface for logging access attempt events in PostgreSQL's object access testing framework. It calls emit_audit_message with a fixed "attempting" type parameter, making the code more readable and maintainable by providing a clear indication that this represents the beginning of an access control check. This function is used throughout the test_oat_hooks module to record when various security hooks are triggered and about to evaluate permissions.

## Parameters / Member Variables
- `hook`: The name of the hook being triggered (e.g., "object_access_hook_str", "exec_check_perms")
- `action`: A dynamically allocated string describing the specific action being attempted (passed through to emit_audit_message for cleanup)
- `objName`: A dynamically allocated string containing the object name being accessed (passed through to emit_audit_message for cleanup, may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - emit_audit_message() (core audit message emission function)
- Called from (representative examples):
  - REGRESS_object_access_hook_str() (for string-based object access hooks)
  - REGRESS_object_access_hook() (for OID-based object access hooks)
  - REGRESS_exec_check_perms() (for executor permission checks)
  - REGRESS_utility_command() (for utility command hooks)

## Notes and Other Information
- Part of the test_oat_hooks module for mandatory access control (MAC) testing
- Provides semantic clarity by using "attempting" as the message type
- Located at src/test/modules/test_oat_hooks/test_oat_hooks.c:259-264
- Forms a trio with audit_success() and audit_failure() for complete audit trail
- Memory management for action and objName parameters is handled by emit_audit_message()