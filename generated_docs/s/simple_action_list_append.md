# simple_action_list_append

## Location
src/bin/psql/startup.c: 748 - 773

## Overview
A static utility function that appends a new action item to the end of a SimpleActionList structure, used by psql's startup processing to queue command-line options and actions.

## Definition


## Detailed Description
This function implements a linked list append operation for psql's action queue system. It creates a new SimpleActionListCell node, initializes it with the provided action type and optional value string, and appends it to the tail of the linked list. The function handles memory allocation for the new cell and performs string duplication if a value is provided, ensuring proper memory management for the action queue.

## Parameters / Member Variables
- : Pointer to the SimpleActionList structure that maintains the linked list
- : Enumerated action type from enum _actions specifying what operation to perform
- : Optional string value associated with the action (copied if not NULL)

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc_object (memory allocation)
  - [pg_strdup](../p/pg_strdup.md) (string duplication)
- Called from (representative examples):
  - [adhoc_opts](../a/adhoc_opts.md)
  - [main](../m/main.md)
  - [parse_psql_options](../p/parse_psql_options.md)

## Notes and Other Information
- This is a static function used internally within startup.c for psql command processing
- The function safely handles NULL values by checking before string duplication
- Memory for both the cell and string value is allocated and managed by PostgreSQL's memory management system
- The function maintains proper linked list invariants by updating both head and tail pointers appropriately