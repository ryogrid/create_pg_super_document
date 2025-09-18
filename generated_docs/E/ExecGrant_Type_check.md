# ExecGrant_Type_check

## Location
src/backend/catalog/aclchk.c: 2444 - 2471

## Overview
ExecGrant_Type_check validates that GRANT and REVOKE operations are performed only on appropriate data types and prevents privilege operations on dependent types.

## Definition


## Detailed Description
ExecGrant_Type_check serves as an object-specific validation callback for data types in the GRANT/REVOKE system. It enforces several important restrictions: preventing privilege operations on array types (since privileges should be set on the element type), multirange types (privileges should be set on the underlying range type), and ensuring that GRANT DOMAIN is only used on actual domain types.

This function maintains PostgreSQL's type system integrity by preventing confusing or meaningless privilege assignments on derived types and ensuring proper use of the DOMAIN-specific grant syntax.

## Parameters / Member Variables
- : Internal representation of the GRANT/REVOKE statement, used to check the intended object type
- : HeapTuple from pg_type catalog containing the type definition

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_type (catalog form structure)
  - GETSTRUCT (tuple access macro)
  - IsTrueArrayType (type checking function)
  - TYPTYPE_MULTIRANGE, TYPTYPE_DOMAIN (type category constants)
  - OBJECT_DOMAIN (object type constant)
  - ereport, errcode, errmsg, errhint (error reporting)
- Called from:
  - ExecGrantStmt_oids (when processing type privileges)
  - ExecGrant_common (as object_check callback)

## Notes and Other Information
- Prevents GRANT on array types - privileges should be set on the element type instead
- Prevents GRANT on multirange types - privileges should be set on the range type instead  
- Validates that GRANT DOMAIN syntax is only used on actual domain types (typtype = TYPTYPE_DOMAIN)
- Part of PostgreSQL's type privilege system that allows USAGE privileges on types
- Helps maintain clear privilege semantics by preventing operations on derived types