# ExecGrant_Language_check

## Location
src/backend/catalog/aclchk.c: 2292 - 2307

## Overview
ExecGrant_Language_check is a validation function that ensures GRANT and REVOKE operations are only performed on trusted procedural languages.

## Definition


## Detailed Description
ExecGrant_Language_check serves as an object-specific validation callback for procedural languages in the GRANT/REVOKE system. It examines the language's trust status and prevents privilege operations on untrusted languages, since only superusers can use untrusted languages and therefore privilege management would be meaningless.

This function is called by ExecGrant_common during language privilege operations to enforce PostgreSQL's security model where untrusted languages require superuser privileges regardless of explicit grants.

## Parameters / Member Variables
- : Internal representation of the GRANT/REVOKE statement (not directly used in this function)
- : HeapTuple from pg_language catalog containing the language definition

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_language (catalog form structure)
  - GETSTRUCT (tuple access macro)
  - ereport, errcode, errmsg, errdetail (error reporting)
- Called from:
  - ExecGrantStmt_oids (when processing language privileges)
  - ExecGrant_common (as object_check callback)

## Notes and Other Information
- Only allows GRANT/REVOKE on trusted languages (lanpltrusted = true)
- Untrusted languages are restricted because they require superuser privileges by design
- Provides specific error message explaining why untrusted languages cannot have privileges managed
- Part of PostgreSQL's security model that separates trusted and untrusted procedural languages