# ObjectAccessType

## Location
[src/include/catalog/objectaccess.h:56-68](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/catalog/objectaccess.h#L56-L68)

## Overview
ObjectAccessType is an enumeration that defines different types of object access events in PostgreSQL's security and logging infrastructure, used by object access hooks to determine when and how to intercept database operations.

## Definition


## Detailed Description
The ObjectAccessType enum serves as the foundation for PostgreSQL's object access hook mechanism, which provides infrastructure for security plugins and logging extensions. Each enum value represents a specific database operation lifecycle event where hooks can be invoked to perform additional security checks, logging, or other custom processing.

The enum is designed to capture key moments in object lifecycle management:
- Object creation completion (OAT_POST_CREATE)
- Object deletion initiation (OAT_DROP) 
- Object modification completion (OAT_POST_ALTER)
- Namespace/schema access attempts (OAT_NAMESPACE_SEARCH)
- Function execution attempts (OAT_FUNCTION_EXECUTE)
- Table truncation attempts (OAT_TRUNCATE)

## Parameters / Member Variables
- : Invoked after object creation, typically after inserting catalog records and dependencies
- : Invoked before object deletion, typically in deleteOneObject()
- : Invoked after object alteration but before command counter increment
- : Invoked before object name lookup in a namespace (equivalent to schema usage permission)
- : Invoked before function execution (equivalent to execute permission)
- : Invoked before table truncation (equivalent to truncate permission)

## Dependencies
- Functions called/Symbols referenced: None (this is a basic enum type)
- Called from (representative examples):
  - [ObjectAccessNamespaceSearch](ObjectAccessNamespaceSearch.md)
  - [REGRESS_object_access_hook_str](../R/REGRESS_object_access_hook_str.md)
  - [REGRESS_object_access_hook](../R/REGRESS_object_access_hook.md)
  - [accesstype_to_string](../a/accesstype_to_string.md)
  - [accesstype_arg_to_string](../a/accesstype_arg_to_string.md)

## Notes and Other Information
- The hook system allows extensions to use different MVCC snapshots (current snapshot for old tuple version, SnapshotSelf for new tuple version) depending on the access type
- The command counter state varies by access type, affecting visibility of changes to hook implementations
- This enum is designed to be extensible with additional access types in future PostgreSQL versions
- Located in src/include/catalog/objectaccess.h:48-56