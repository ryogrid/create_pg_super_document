# check_object_ownership

## Location
[src/backend/catalog/objectaddress.c:2382-2563](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L2382-L2563)

## Overview
Validates that a specified role has ownership privileges on a database object, implementing PostgreSQL's object ownership security model with type-specific permission checks.

## Definition


## Detailed Description
The `check_object_ownership` function enforces PostgreSQL's ownership-based access control by verifying that a given role has the necessary ownership privileges for a specific database object. This function is central to PostgreSQL's security model, as many administrative operations require object ownership.

The function uses a comprehensive switch statement to handle different object types, each with their own ownership semantics. For most objects, ownership is determined by direct ownership checks, but some objects have special rules - for example, roles require either superuser privileges or CREATEROLE privilege with admin option, while certain system objects like parsers and templates require superuser access.

The function handles complex ownership scenarios such as domain constraints (which check the underlying type ownership), casts (which require ownership of either source or target type), and large objects (with compatibility mode considerations). It provides detailed error messages that specify the exact privilege requirements for each object type.

## Parameters / Member Variables
- `roleid`: OID of the role whose ownership privileges are being checked
- `objtype`: Enumerated type indicating the kind of object being checked (table, function, etc.)
- `address`: ObjectAddress structure containing classId, objectId, and objectSubId
- `object`: Node structure containing the parsed object specification  
- `relation`: Open relation reference (for relation-based objects), may be NULL

## Dependencies
- Functions called/Symbols referenced:
  - [object_ownercheck](../o/object_ownercheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [aclcheck_error_type](../a/aclcheck_error_type.md)
  - superuser_arg
  - [has_createrole_privilege](../h/has_createrole_privilege.md)
  - is_admin_of_role
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [typenameTypeId](../t/typenameTypeId.md)
  - [format_type_be](../f/format_type_be.md)
  - [GetUserNameFromId](../G/GetUserNameFromId.md)
  - [NameListToString](../N/NameListToString.md)
- Called from (representative examples):
  - [ExecAlterObjectDependsStmt](../E/ExecAlterObjectDependsStmt.md) (src/backend/commands/alter.c:475)
  - [CommentObject](../C/CommentObject.md) (src/backend/commands/comment.c:76)
  - [RemoveObjects](../R/RemoveObjects.md) (src/backend/commands/dropcmds.c:105)
  - [ExecAlterExtensionContentsStmt](../E/ExecAlterExtensionContentsStmt.md) (src/backend/commands/extension.c:3349)
  - [ExecSecLabelStmt](../E/ExecSecLabelStmt.md) (src/backend/commands/seclabel.c:172)

## Notes and Other Information
- Supports over 30 different object types with type-specific ownership rules
- Role ownership has special semantics: superusers can only be owned by other superusers, while regular roles require CREATEROLE privilege plus admin option
- Some object types (OBJECT_AMOP, OBJECT_AMPROC, etc.) are explicitly unsupported and will cause an error
- Large object ownership checks respect the lo_compat_privileges setting for backward compatibility
- For casts, the function checks ownership of either the source OR target type (not both required)
- Domain constraint ownership is checked via the underlying domain type ownership
- Provides context-specific error messages that help users understand required privileges