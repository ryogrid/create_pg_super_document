# RelFileLocatorBackend

## Location
[src/include/storage/relfilelocator.h:73-77](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/relfilelocator.h#L73-L77)

## Overview
RelFileLocatorBackend extends RelFileLocator by adding backend process number information, providing complete identification for both regular and backend-local relation storage.

## Definition

```c
typedef struct RelFileLocatorBackend
{
	RelFileLocator locator;
	ProcNumber	backend;
} RelFileLocatorBackend;
```
## Detailed Description
The RelFileLocatorBackend struct augments the basic RelFileLocator with a backend process number, creating a complete identifier for locating physical storage files. This extended structure is essential for distinguishing between regular relations (accessible to multiple backends) and backend-local relations (private to a single backend process).

The backend field serves two primary purposes:
- For regular relations: Contains INVALID_PROC_NUMBER, indicating the relation is accessible to multiple backends
- For backend-local relations: Contains the owning backend's process number, making the relation private to that specific backend

Backend-local relations have special characteristics:
- Always transient and automatically removed during database crashes
- Never WAL-logged or fsync'd due to their temporary nature
- Used for temporary tables, intermediate query results, and other ephemeral data

The structure includes utility macros for comparison and testing:
- RelFileLocatorBackendEquals() for comparing two RelFileLocatorBackend structs
- RelFileLocatorBackendIsTemp() for testing if a relation is backend-local

## Parameters / Member Variables
- : Embedded RelFileLocator struct containing tablespace (spcOid), database (dbOid), and relation number (relNumber)
- : Process number of the owning backend; INVALID_PROC_NUMBER for regular relations, specific ProcNumber for backend-local relations

## Dependencies
- Functions called/Symbols referenced:
  - [RelFileLocator](RelFileLocator.md) (embedded struct)
  - ProcNumber (process identifier type)
- Called from (representative examples):
  - [smgropen](../s/smgropen.md) (storage manager relation opening)
  - [DropRelationBuffers](../D/DropRelationBuffers.md) (buffer management)
  - [mdunlink](../m/mdunlink.md)/mdunlinkfork (relation file unlinking)
  - Cache invalidation functions

## Notes and Other Information
- Essential for PostgreSQL's storage manager and buffer management systems
- The backend field determines relation visibility and lifecycle management
- [Backend](../B/Backend.md)-local relations are automatically cleaned up on backend termination or crash
- Used extensively in storage manager operations, buffer pool management, and cache invalidation
- Supports both persistent (multi-backend) and temporary (single-backend) relation storage patterns