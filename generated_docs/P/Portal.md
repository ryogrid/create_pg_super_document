# Portal

## Location
[src/include/utils/portal.h:113-114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/portal.h#L113-L114)

## Overview
Portal is a pointer type that represents a query execution context in PostgreSQL, providing a handle to a PortalData structure for managing SQL statement execution lifecycle.

## Definition


## Detailed Description
Portal is a fundamental abstraction in PostgreSQL's query execution system that serves as a handle for managing the execution of SQL statements. It is essentially a pointer to a PortalData structure, which contains all the necessary information and state for executing queries. Portals provide a unified interface for handling different types of queries including simple queries, prepared statements, and cursors. The portal system manages the complete lifecycle of query execution from preparation through completion, including resource management, transaction handling, and result set management.

## Parameters / Member Variables
- N/A (This is a typedef for a pointer to PortalData)

## Dependencies
- Functions called/Symbols referenced:
  - [PortalData](PortalData.md) (the underlying structure this points to)
- Called from (representative examples):
  - [PerformCursorOpen](PerformCursorOpen.md)
  - [PerformPortalFetch](PerformPortalFetch.md)  
  - [PortalStart](PortalStart.md)
  - [PortalRun](PortalRun.md)
  - CreatePortal
  - PortalDrop
  - [exec_simple_query](../e/exec_simple_query.md)
  - [exec_execute_message](../e/exec_execute_message.md)
  - [SPI_cursor_open](../S/SPI_cursor_open.md)
  - Various portal management functions in portalmem.c

## Notes and Other Information
- [Portal](Portal.md) is widely used throughout PostgreSQL's backend for query execution management
- The portal system supports different execution strategies (PORTAL_ONE_SELECT, PORTAL_ONE_RETURNING, etc.)
- Portals can be held across transaction boundaries for cursor functionality
- [Portal](Portal.md) lifecycle includes states: NEW, DEFINED, READY, ACTIVE, DONE, FAILED
- Memory management for portals is handled through dedicated portal memory contexts
- Portals are used by both direct SQL execution and the SPI (Server Programming Interface)
- The portal hash table manages all active portals in a session