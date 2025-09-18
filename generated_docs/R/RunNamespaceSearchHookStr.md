# RunNamespaceSearchHookStr

## Location
src/backend/catalog/objectaccess.c: 241 - 264

## Overview
Invokes the string-based object access hook for namespace search operations, allowing extensions to control access to PostgreSQL schemas by object name rather than OID.

## Definition
```c
bool RunNamespaceSearchHookStr(const char *objectName, bool ereport_on_violation)
```

## Detailed Description
This function serves as the entrypoint for the OAT_NAMESPACE_SEARCH object access hook when working with object names as strings instead of OIDs. It provides a mechanism for PostgreSQL extensions to intercept and potentially deny schema search operations based on the schema name. The function initializes an ObjectAccessNamespaceSearch structure with appropriate parameters and invokes the registered string-based object access hook (object_access_hook_str). Extensions can use this hook to implement custom access control policies for namespace searches.

The function follows PostgreSQL's object access hook pattern, where extensions register hook functions that are called at specific points during database operations. This particular hook is called during namespace (schema) search operations when the system needs to determine if access to a particular schema should be allowed.

## Parameters / Member Variables
- `objectName`: The name of the namespace (schema) being searched as a C string
- `ereport_on_violation`: Boolean flag indicating whether the hook should report an error when access is denied (true) or silently deny access (false)

## Dependencies
- Functions called/Symbols referenced:
  - ObjectAccessNamespaceSearch (struct type for hook arguments)
  - OAT_NAMESPACE_SEARCH (object access type constant)
  - object_access_hook_str (global hook function pointer)
  - NamespaceRelationId (system catalog relation ID constant)
- Called from (representative examples):
  - InvokeNamespaceSearchHookStr (wrapper macro/function)

## Notes and Other Information
- Requires that object_access_hook_str is not NULL (checked by assertion)
- Returns the result from the hook execution (true allows access, false denies it)
- The hook argument structure is zero-initialized before use to ensure clean state
- Part of PostgreSQL's extensible object access control system that allows third-party extensions to implement custom security policies
- This is the string-based variant; there's also RunNamespaceSearchHook() that works with OIDs
- Extensions should be careful to only set result to false when denying access, never to true, to ensure multiple extensions work together correctly