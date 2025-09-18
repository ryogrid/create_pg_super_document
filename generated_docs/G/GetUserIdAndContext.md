# GetUserIdAndContext

## Location
src/backend/utils/init/miscinit.c: 707 - 713

## Overview
An obsolete compatibility function that retrieves the current user ID and local user ID change status, maintained for backward compatibility with external code such as PL/Java.

## Definition
```c
void GetUserIdAndContext(Oid *userid, bool *sec_def_context)
```

## Detailed Description
GetUserIdAndContext is a legacy function provided primarily for bug-compatibility with external PostgreSQL extensions, particularly PL/Java. Unlike the newer GetUserIdAndSecContext function, this version returns a simplified boolean flag indicating whether a local user ID change is active, rather than the full security restriction context.

This function is considered obsolete in favor of GetUserIdAndSecContext, which provides more comprehensive security context information. The function exists to maintain compatibility with older external code that depends on the previous API interface.

## Parameters / Member Variables
- `userid`: Pointer to Oid variable that will receive the current effective user ID  
- `sec_def_context`: Pointer to bool variable that will receive true if inside a local user ID change operation

## Dependencies
- Functions called/Symbols referenced:
  - CurrentUserId (global variable)
  - InLocalUserIdChange (function)
- Called from (representative examples):
  - AmSpecialWorkerProcess

## Notes and Other Information
- Marked as obsolete in PostgreSQL source code comments
- Maintained for backward compatibility with PL/Java and potentially other extensions
- Returns simplified boolean context information rather than full security restriction flags
- Uses InLocalUserIdChange() to determine the security context flag value
- Should not be used in new PostgreSQL code - GetUserIdAndSecContext is preferred
- Part of PostgreSQL's strategy to maintain external extension compatibility while evolving internal APIs