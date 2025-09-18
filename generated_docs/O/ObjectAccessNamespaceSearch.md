# ObjectAccessNamespaceSearch

## Location
[src/include/catalog/objectaccess.h:124-172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/catalog/objectaccess.h#L124-L172)

## Overview
ObjectAccessNamespaceSearch is a struct that holds arguments for the OAT_NAMESPACE_SEARCH object access hook event, providing a mechanism for extensions to control and monitor namespace access permissions in PostgreSQL.

## Definition


## Detailed Description
The ObjectAccessNamespaceSearch struct serves as a parameter container for object access hooks that are triggered during namespace (schema) search operations (OAT_NAMESPACE_SEARCH events). This struct implements a cooperative security model where multiple extensions can participate in access control decisions.

The struct operates on a consensus-based approach where:
1. Core PostgreSQL code initializes the  field to  (access granted by default)
2. Each extension hook can examine the namespace search request and set  to  if access should be denied
3. Extensions should never set  back to , ensuring that if any extension denies access, the operation is blocked
4. The  flag controls whether an error should be reported when access is denied

This design ensures that multiple security extensions can work together without conflicts, and access is only granted when all extensions agree.

## Parameters / Member Variables
- : Boolean flag indicating whether the hook should report an error when permission to search the schema is denied. When true, access denial results in an error message to the user.
- : Boolean out parameter that determines the final access decision. Core code initializes this to true, and extensions should only set it to false to deny access. Multiple extensions must all agree for access to be granted.

## Dependencies
- Functions called/Symbols referenced:
  - [ObjectAccessType](ObjectAccessType.md) (used in hook function signatures)
  - Various RunNamespaceSearch* functions
- Called from (representative examples):
  - [RunNamespaceSearchHook](../R/RunNamespaceSearchHook.md)
  - [RunNamespaceSearchHookStr](../R/RunNamespaceSearchHookStr.md)
  - [accesstype_arg_to_string](../a/accesstype_arg_to_string.md)

## Notes and Other Information
- This struct is specifically used with OAT_NAMESPACE_SEARCH hook events
- Implements a cooperative security model where multiple extensions can participate in access decisions
- Extensions must follow the protocol of never setting result to true, only to false
- The consensus approach ensures that access is only granted when all security extensions agree
- Equivalent to usage permission on schemas under the default PostgreSQL access control mechanism
- Part of PostgreSQL's object access hook infrastructure for security and audit extensions
- Located in src/include/catalog/objectaccess.h:108-124