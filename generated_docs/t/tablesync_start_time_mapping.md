# tablesync_start_time_mapping

## Location
src/backend/replication/logical/tablesync.c: 420 - 692

## Overview
A lightweight structure used internally by PostgreSQL's logical replication system to track the last start time of table synchronization workers, preventing immediate restarts and implementing rate limiting for worker launches.

## Definition


## Detailed Description
The  struct serves as a hash table entry within the  function to implement a throttling mechanism for table synchronization workers in PostgreSQL's logical replication. This structure is part of a rate-limiting strategy that prevents the system from continuously attempting to restart failed table sync workers, which could lead to resource exhaustion or performance degradation.

The struct is used as the entry type for a static hash table () that maintains a mapping between table OIDs and their most recent worker start times. Before launching a new table sync worker, the system checks this hash table to ensure that sufficient time (controlled by ) has elapsed since the last attempt, preventing rapid succession of worker launches for the same table.

## Parameters / Member Variables
- : The object identifier (OID) of the relation (table) being synchronized. This serves as the hash key for lookup operations.
- : A timestamp with timezone indicating when a table sync worker was last started for this relation. Used to calculate whether enough time has passed to allow another worker launch attempt.

## Dependencies
- Functions called/Symbols referenced:
  - : Used to create the hash table with this struct as entry type
  - : Used to find or create entries in the hash table
  - : Used to clean up the hash table when no longer needed
  - : Used to obtain current time for comparison
  - : Used to check if sufficient time has elapsed

- Called from (representative examples):
  - : The only function that directly uses this struct, as it's defined locally within that function's scope

## Notes and Other Information
- This is a local struct definition within the  function in 
- The struct is used exclusively as a hash table entry type and is not exposed outside its containing function
- The hash table using this struct is created only when there are tables that need syncing () and is destroyed when all tables are synchronized
- The rate limiting mechanism implemented with this struct respects the  configuration parameter
- This struct is part of PostgreSQL's logical replication worker management system, specifically for handling table synchronization workers that may fail and need to be restarted