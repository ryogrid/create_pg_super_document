# aclinsert

## Location
[src/backend/utils/adt/acl.c:1592-1601](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L1592-L1601)

## Overview
A deprecated PostgreSQL function that previously supported ACL manipulation but now raises an error indicating it is no longer supported.

## Definition

```c
Datum
aclinsert(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a legacy PostgreSQL function that was once exported for manipulating Access Control Lists. In current PostgreSQL versions, this function has been deprecated and removed from active use. When called, it immediately raises a FEATURE_NOT_SUPPORTED error indicating that the functionality is no longer available.

This function represents part of PostgreSQL's evolution away from direct ACL manipulation functions toward more structured privilege management through GRANT/REVOKE SQL commands and internal ACL processing functions.

## Parameters / Member Variables
- Uses  macro - function arguments would have been passed through PostgreSQL's function call convention

## Dependencies
- Functions called/Symbols referenced:
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - PG_RETURN_NULL
  - ERRCODE_FEATURE_NOT_SUPPORTED
- Called from (representative examples):
  - No current references (function is deprecated)

## Notes and Other Information
- Exported function (was available to SQL layer in older PostgreSQL versions)
- Always raises ERROR with ERRCODE_FEATURE_NOT_SUPPORTED
- Part of deprecated ACL manipulation API that has been replaced by internal functions
- Maintained for backward compatibility but provides no actual functionality
- The PG_RETURN_NULL() at the end is never reached due to the ERROR, but kept for compiler satisfaction
- Represents PostgreSQL's approach to deprecating functions while maintaining ABI stability