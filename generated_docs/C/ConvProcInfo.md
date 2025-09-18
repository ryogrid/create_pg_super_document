# ConvProcInfo

## Location
src/backend/utils/mb/mbutils.c: 53 - 59

## Overview
ConvProcInfo is a structure that caches function manager lookup information for character encoding conversion functions between server and client encodings in PostgreSQL.

## Definition


## Detailed Description
ConvProcInfo is a caching structure used in PostgreSQL's multi-byte character encoding system. It maintains a simple linked list that stores function manager (fmgr) lookup information for the currently selected conversion functions, as well as any that have been selected previously in the current session. This caching mechanism is crucial for performance optimization as it avoids repeated catalog lookups for encoding conversion functions.

The structure is designed to support transaction rollback scenarios where PostgreSQL must be able to restore a previous encoding setting without performing fresh catalog accesses. All ConvProcInfo data is kept in TopMemoryContext and is never released during the session lifetime.

## Parameters / Member Variables
- : Integer identifier for the server's character encoding
- : Integer identifier for the client's character encoding  
- : FmgrInfo structure containing lookup information for the conversion function that converts from client encoding to server encoding
- : FmgrInfo structure containing lookup information for the conversion function that converts from server encoding to client encoding

## Dependencies
- Functions called/Symbols referenced:
  - [FmgrInfo](../F/FmgrInfo.md) (PostgreSQL function manager info structure)
- Called from (representative examples):
  - [PrepareClientEncoding](../P/PrepareClientEncoding.md) (at src/backend/utils/mb/mbutils.c:142, 157, 158, 190)
  - [SetClientEncoding](../S/SetClientEncoding.md) (at src/backend/utils/mb/mbutils.c:248)

## Notes and Other Information
- The structure is part of a caching system that maintains conversion function information across the session
- Data is stored in TopMemoryContext to persist throughout the session lifetime
- The caching mechanism supports transaction rollback by preserving previous encoding settings
- This is a private structure defined within src/backend/utils/mb/mbutils.c and is used internally by PostgreSQL's multi-byte utilities
- The structure enables efficient character encoding conversions by avoiding repeated function lookups from the system catalogs