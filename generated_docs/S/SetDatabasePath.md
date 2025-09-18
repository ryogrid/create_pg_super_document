# SetDatabasePath

## Location
src/backend/utils/init/miscinit.c: 329 - 341

## Overview
Sets the global database path variable for the current PostgreSQL backend process, ensuring it's only set once per process lifetime.

## Definition
```c
void SetDatabasePath(const char *path)
```

## Detailed Description
SetDatabasePath is a critical initialization function that establishes the file system path to the database directory for the current backend process. The function performs a one-time assignment of the global DatabasePath variable, which is used throughout the backend's lifetime to locate database files. It includes an assertion to ensure the path is only set once per process, preventing accidental overwrites that could lead to data corruption or inconsistent state.

## Parameters / Member Variables
- `path`: A null-terminated string containing the file system path to the database directory

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextStrdup (for creating a persistent copy of the path string)
  - Assert (for ensuring single assignment)
  - DatabasePath (global variable being set)
  - TopMemoryContext (memory context for persistent allocation)
- Called from (representative examples):
  - InitPostgres
  - AmSpecialWorkerProcess

## Notes and Other Information
This function is part of PostgreSQL's database initialization sequence and is crucial for establishing the backend's file system context. The use of TopMemoryContext ensures that the database path remains valid for the entire lifetime of the backend process. The single-assignment restriction (enforced by the Assert) is a safety measure that prevents programming errors that could result in the backend operating on the wrong database directory.