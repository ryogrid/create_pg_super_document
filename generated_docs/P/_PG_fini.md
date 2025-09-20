# _PG_fini

## Location
[src/test/modules/ldap_password_func/ldap_password_func.c:41-46](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/ldap_password_func/ldap_password_func.c#L41-L46)

## Overview
Module cleanup function for PostgreSQL extensions that is called when the module is unloaded from the server.

## Definition

```c
void
_PG_fini(void)
```
## Detailed Description
The _PG_fini function is a special function that PostgreSQL calls automatically when a dynamically loaded module is being unloaded from the server. This provides an opportunity for the module to perform cleanup operations such as deallocating resources, unhooking from system callbacks, or resetting global state.

In this specific implementation from the ldap_password_func test module, the function currently performs no operations (contains only a comment "do nothing yet"). This is common for simple test modules or modules that don't require explicit cleanup since PostgreSQL handles most resource cleanup automatically.

The _PG_fini function is the counterpart to _PG_init and together they provide the module lifecycle management hooks for PostgreSQL extensions.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - None (function body is empty)
- Called from (representative examples):
  - PostgreSQL dynamic module unloading system
  - PG_FUNCTION_INFO_V1 infrastructure

## Notes and Other Information
- This is a standard PostgreSQL extension cleanup function that may be present in dynamically loaded modules
- The function is optional - modules can omit _PG_fini if no cleanup is needed
- Located in src/test/modules/ldap_password_func/ldap_password_func.c:41-46
- Part of a test module for LDAP password transformation functionality
- Currently has no implementation but serves as a placeholder for future cleanup needs
- The corresponding _PG_init function in this module sets up an LDAP password hook