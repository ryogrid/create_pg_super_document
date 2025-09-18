# GetUserMapping

## Location
src/backend/foreign/foreign.c: 200 - 253

## Overview
Retrieves a user mapping for a specific user and foreign server, with fallback to PUBLIC mappings if user-specific mapping is not found.

## Definition
```c
UserMapping *GetUserMapping(Oid userid, Oid serverid)
```

## Detailed Description
GetUserMapping is a core function in PostgreSQL's foreign data wrapper system that locates and returns user mapping information for connecting to foreign servers. The function implements a two-tier lookup strategy: first searching for a mapping specific to the given user ID, and if not found, falling back to a PUBLIC mapping (userid == InvalidOid). If neither mapping exists, it raises an error with a descriptive message. The function constructs a complete UserMapping structure containing the mapping ID, user ID, server ID, and processed options extracted from the system catalog.

## Parameters / Member Variables
- `userid`: Object ID of the user for whom to find the mapping
- `serverid`: Object ID of the foreign server

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache2
  - GetForeignServer
  - MappingUserName
  - SysCacheGetAttr
  - untransformRelOptions
  - palloc
  - ereport/errmsg/errcode
- Called from (representative examples):
  - Foreign data wrapper connection establishment routines
  - User mapping validation functions

## Notes and Other Information
The function uses the system cache USERMAPPINGUSERSERVER for efficient lookup. The PUBLIC mapping fallback mechanism allows administrators to define default connection parameters for all users of a foreign server. Options are stored in a transformed format and need to be untransformed using untransformRelOptions() before use. The function is located in src/backend/foreign/foreign.c:200-253 and is essential for FDW authentication and connection parameter resolution.