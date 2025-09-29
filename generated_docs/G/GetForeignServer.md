# GetForeignServer

## Location
[src/backend/foreign/foreign.c:111-122](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/foreign/foreign.c#L111-L122)

## Overview
Retrieves a foreign server object by its Object ID (OID), providing a simplified interface to access foreign server information without error handling options.

## Definition
```c
ForeignServer *GetForeignServer(Oid serverid)
```

## Detailed Description
GetForeignServer is a wrapper function that provides a convenient interface to look up foreign server objects by their OID. It internally calls GetForeignServerExtended with default flags (0), which means it will raise an error if the specified foreign server cannot be found. This function is commonly used throughout the PostgreSQL codebase when foreign server information is needed and the caller expects the server to exist. Foreign servers represent connections to external data sources through foreign-data wrappers.

## Parameters / Member Variables
- `serverid`: The Object ID (OID) of the foreign server to retrieve

## Dependencies
- Functions called/Symbols referenced:
  - [GetForeignServerExtended](GetForeignServerExtended.md) (extended server lookup function)
  - [ForeignServer](../F/ForeignServer.md) (return type structure)
- Called from (representative examples):
  - [getObjectDescription](../g/getObjectDescription.md)
  - [getObjectIdentityParts](../g/getObjectIdentityParts.md)
  - [ATExecAlterColumnGenericOptions](../A/ATExecAlterColumnGenericOptions.md)
  - [ATExecGenericOptions](../A/ATExecGenericOptions.md)
  - [GetForeignServerByName](GetForeignServerByName.md)
  - [GetUserMapping](GetUserMapping.md)

## Notes and Other Information
- Located in src/backend/foreign/foreign.c:111-122
- This is a convenience wrapper around GetForeignServerExtended with flags set to 0
- Will raise an ERROR if the foreign server with the specified OID does not exist
- For cases where you need to handle missing servers gracefully, use GetForeignServerExtended directly with FSV_MISSING_OK flag
- Returns a palloc'd ForeignServer structure that should be freed by the caller when no longer needed
- Foreign servers define connection parameters and options for accessing external data sources

## Simplified Source

```c
ForeignServer *
GetForeignServer(Oid serverid)
{
    // Simple wrapper that calls the extended version with default flags
    return GetForeignServerExtended(serverid, 0);
}
```