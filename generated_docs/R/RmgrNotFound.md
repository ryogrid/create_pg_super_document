# RmgrNotFound

## Location
src/backend/access/transam/rmgr.c: 91 - 106

## Overview
Emits an error when encountering a WAL record with an unregistered resource manager ID.

## Definition
void RmgrNotFound(RmgrId rmid)

## Detailed Description
RmgrNotFound is called when the WAL recovery process encounters a WAL record with a resource manager ID that is not registered in the system. This function generates a descriptive error message that includes the unknown resource manager ID and provides a helpful hint about loading the required extension module.

The function uses ereport() to emit an ERROR level message, which will abort the current transaction and provide detailed information to help users diagnose and fix the issue. This typically occurs when trying to recover WAL records from an extension that was previously loaded but is not currently in shared_preload_libraries.

## Parameters / Member Variables
- rmid: The unrecognized resource manager ID that was encountered in a WAL record

## Dependencies
- Functions called/Symbols referenced:
  - RmgrId
  - ereport
  - [errmsg](../e/errmsg.md)
  - [errhint](../e/errhint.md)
- Called from (representative examples):
  - GetRmgr

## Notes and Other Information
- Located in src/backend/access/transam/rmgr.c:91-106
- This function always throws an ERROR and does not return normally
- The error message specifically suggests adding the required extension to shared_preload_libraries
- This is a critical part of WAL recovery error handling that helps users identify missing custom resource manager extensions
- The function is designed to provide actionable guidance for resolving the error condition