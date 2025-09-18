# am_tablesync_worker

## Location
src/include/replication/worker_internal.h: 334 - 339

## Overview
A convenience function that determines if the current logical replication worker is operating as a table synchronization worker.

## Definition


## Detailed Description
The  function is a simple inline wrapper that checks whether the current logical replication worker (represented by the global ) is configured as a table synchronization worker. This function provides a clean interface for determining the worker type without directly accessing the worker structure fields.

Table synchronization workers are responsible for performing initial synchronization of table data when setting up logical replication. They handle the initial bulk copy of existing data from the publisher to the subscriber before switching to incremental replication.

The function internally calls the  macro, which checks both that the worker is in use and that its type is .

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  -  (macro)
  -  (global variable)
  -  (enum value)

- Called from (representative examples):
  -  (src/backend/replication/logical/tablesync.c:1673)
  -  (src/backend/replication/logical/worker.c:1049)
  -  (src/backend/replication/logical/worker.c:1288)
  -  (src/backend/replication/logical/worker.c:1418)
  -  (src/backend/replication/logical/worker.c:4464)
  -  (src/backend/replication/logical/worker.c:4665)
  -  (src/backend/replication/logical/worker.c:4696)
  -  (src/backend/replication/logical/worker.c:4781)

## Notes and Other Information
- This is an inline function defined in the header file src/include/replication/worker_internal.h
- The function serves as a type safety wrapper around the  macro
- Table synchronization workers are distinct from apply workers and parallel apply workers in PostgreSQL's logical replication architecture
- The function is used throughout the logical replication codebase to branch logic based on worker type