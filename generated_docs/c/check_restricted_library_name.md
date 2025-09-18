# check_restricted_library_name

## Location
[src/backend/utils/fmgr/dfmgr.c:469-483](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/dfmgr.c#L469-L483)

## Overview
Validates that a library name follows the security restrictions for accessing shared libraries in PostgreSQL.

## Definition


## Detailed Description
This function enforces security restrictions on library names to prevent unauthorized access to system libraries or directory traversal attacks. It implements a strict validation policy that requires restricted library names to:

1. Begin with the exact prefix "$libdir/plugins/"
2. Contain no additional directory separators after the plugins/ directory

The function uses  to verify the required prefix and  to ensure no subdirectory navigation is attempted. If either check fails, it raises an ERROR with , effectively blocking the library load operation.

This security mechanism prevents malicious code from accessing libraries outside the designated plugins directory, protecting against path traversal attacks like "../../../etc/passwd" or similar attempts to access unauthorized system resources.

## Parameters / Member Variables
- : The library name to validate for security compliance

## Dependencies
- Functions called/Symbols referenced:
  -  - locates directory separators in the path
  -  - compares string prefixes
  -  - PostgreSQL's error reporting mechanism
- Called from:
  -  (src/backend/utils/fmgr/dfmgr.c:150)

## Notes and Other Information
- The function is static, limiting its scope to the dfmgr.c compilation unit
- Uses PostgreSQL's error reporting system with  and specific error codes
- The "$libdir/plugins/" prefix ensures libraries are loaded only from the designated plugin directory
- Prevents ".." style directory traversal attacks by disallowing any directory separators after the plugins/ directory
- Part of PostgreSQL's security framework for controlling access to external shared libraries
- The error message includes the attempted library name for debugging purposes while maintaining security