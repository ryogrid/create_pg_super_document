# RunNamespaceSearchHook

## Location
[src/backend/catalog/objectaccess.c:115-138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaccess.c#L115-L138)

## Overview
Executes registered object access hooks for namespace search events, allowing extensions to control access to schemas and return a boolean result indicating whether access should be granted.

## Definition
```c
bool RunNamespaceSearchHook(Oid objectId, bool ereport_on_violation)
```

## Detailed Description
This function serves as the entry point for the OAT_NAMESPACE_SEARCH object access hook event. Unlike other object access hooks, this function returns a boolean value and is specifically designed for implementing access control policies for namespace (schema) searches. It is called when PostgreSQL needs to determine whether a user has permission to search within a particular schema.

The function constructs an ObjectAccessNamespaceSearch structure with the provided parameters, initializes the result field to true (allowing access by default), and invokes any registered object access hooks. Extensions can modify the result field to deny access. The function implements a conservative approach where access is only granted if all loaded extensions agree to allow it.

## Parameters / Member Variables
- `objectId`: The OID of the namespace (schema) being searched
- `ereport_on_violation`: Boolean flag indicating whether the hook should report an error when permission is denied

## Dependencies
- Functions called/Symbols referenced:
  - [ObjectAccessNamespaceSearch](../O/ObjectAccessNamespaceSearch.md) (struct type)
  - OAT_NAMESPACE_SEARCH (enum value)
  - NamespaceRelationId (constant - OID of pg_namespace catalog, value 2615)
  - object_access_hook (global function pointer)
  - Assert (assertion macro)
  - memset (memory initialization function)

- Called from (representative examples):
  - [ObjectAccessNamespaceSearch](../O/ObjectAccessNamespaceSearch.md)
  - InvokeNamespaceSearchHook

## Notes and Other Information
- This is the only object access hook function that returns a boolean value (access granted/denied)
- The function includes an assertion to ensure object_access_hook is not NULL
- The ObjectAccessNamespaceSearch structure is zero-initialized, then the ereport_on_violation flag is set and result is initialized to true
- Extensions should only set the result field to false (deny access) and never to true, ensuring that access is only granted if all extensions agree
- Always uses NamespaceRelationId (2615) as the classId since this hook is specific to namespace searches
- Always uses 0 as subId since namespace searches apply to entire schemas, not sub-objects
- The ereport_on_violation parameter controls whether permission denied situations should generate user-visible error messages
- This hook is essential for implementing row-level security and custom access control policies for schema-level operations
- Part of PostgreSQL's extensibility framework for fine-grained access control and security policy enforcement