# aclremove

## Location
src/backend/utils/adt/acl.c: 1602 - 1611

## Overview
A deprecated PostgreSQL function that previously supported ACL manipulation but now raises an error indicating it is no longer supported.

## Definition


## Detailed Description
The  function is a legacy PostgreSQL function that was once exported for removing entries from Access Control Lists. In current PostgreSQL versions, this function has been deprecated and removed from active use. When called, it immediately raises a FEATURE_NOT_SUPPORTED error indicating that the functionality is no longer available.

Like its counterpart , this function represents part of PostgreSQL's evolution away from direct ACL manipulation functions toward more structured privilege management through GRANT/REVOKE SQL commands and internal ACL processing functions. The removal of these functions helps enforce proper privilege management through the standard SQL interface.

## Parameters / Member Variables
- Uses  macro - function arguments would have been passed through PostgreSQL's function call convention

## Dependencies
- Functions called/Symbols referenced:
  - ereport
  - errcode
  - errmsg
  - PG_RETURN_NULL
  - ERRCODE_FEATURE_NOT_SUPPORTED
- Called from (representative examples):
  - No current references (function is deprecated)

## Notes and Other Information
- Exported function (was available to SQL layer in older PostgreSQL versions)
- Always raises ERROR with ERRCODE_FEATURE_NOT_SUPPORTED
- Part of deprecated ACL manipulation API that has been replaced by internal functions and SQL REVOKE commands
- Maintained for backward compatibility but provides no actual functionality
- The PG_RETURN_NULL() at the end is never reached due to the ERROR, but kept for compiler satisfaction
- Companion to the similarly deprecated  function
- Represents PostgreSQL's commitment to maintaining ABI stability while deprecating unsafe direct ACL manipulation