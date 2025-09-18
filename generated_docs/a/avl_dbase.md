# avl_dbase

## Location
src/backend/postmaster/autovacuum.c: 167 - 173

## Overview
The  structure is used by the PostgreSQL autovacuum launcher to track database information and manage scheduling of autovacuum workers across different databases.

## Definition


## Detailed Description
The  structure serves as a database tracking entry in the autovacuum launcher's internal data structures. It maintains essential information needed to schedule and prioritize autovacuum operations across multiple databases. The structure is designed to be used in hash tables (with  as the key) and doubly-linked lists (via ) for efficient database management and scheduling algorithms.

## Parameters / Member Variables
- : Database OID that serves as the hash key for this entry (must be the first field for hash table operations)
- : Timestamp indicating when the next autovacuum worker should be scheduled for this database
- : Numeric score used for prioritizing databases when determining which should receive autovacuum attention first
- : Doubly-linked list node for organizing databases in scheduling queues

## Dependencies
- Functions called/Symbols referenced:
  - dlist_node (for linked list operations)
- Called from (representative examples):
  - launcher_determine_sleep
  - rebuild_database_list
  - db_comparator
  - do_start_worker
  - launch_worker

## Notes and Other Information
- The  field must be positioned first in the structure to serve as a proper hash key
- This structure is central to the autovacuum launcher's database scheduling logic
- The scoring system () helps prioritize databases that need more urgent attention
- Used extensively in database list rebuilding and worker launching operations