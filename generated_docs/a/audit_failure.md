# audit_failure

## Location
src/test/modules/test_oat_hooks/test_oat_hooks.c: 271 - 276

## Overview
A static wrapper function in the test_oat_hooks module that logs access denial events for testing object access control hooks.

## Definition
```c
static void audit_failure(const char *hook, char *action, char *objName)
```

## Detailed Description
audit_failure is a semantic wrapper function that provides a clear interface for logging access denial events in PostgreSQL's object access testing framework. It calls emit_audit_message with a fixed "denied" type parameter, indicating that a security hook evaluation has determined that the requested operation should be blocked. This function is crucial for testing mandatory access control (MAC) mechanisms as it provides visibility into when and why access restrictions are being enforced. It completes the audit trail trio along with audit_attempt() and audit_success().

## Parameters / Member Variables
- `hook`: The name of the hook that denied access (e.g., "exec_check_perms")
- `action`: A dynamically allocated string describing the specific action that was denied (passed through to emit_audit_message for cleanup)
- `objName`: A dynamically allocated string containing the object name that access was denied to (passed through to emit_audit_message for cleanup, may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [emit_audit_message](../e/emit_audit_message.md)() (core audit message emission function)
- Called from (representative examples):
  - [REGRESS_exec_check_perms](../R/REGRESS_exec_check_perms.md)() (for executor permission check failures)

## Notes and Other Information
- Part of the test_oat_hooks module for mandatory access control (MAC) testing
- Provides semantic clarity by using "denied" as the message type to indicate access rejection
- Located at src/test/modules/test_oat_hooks/test_oat_hooks.c:271-276
- Works with audit_attempt() and audit_success() to provide complete access control audit logging
- Currently used primarily by executor permission checking, but available for other hook types
- Memory management for action and objName parameters is handled by emit_audit_message()
- Essential for verifying that security policies correctly block unauthorized access attempts