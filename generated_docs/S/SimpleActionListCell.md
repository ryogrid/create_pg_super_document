# SimpleActionListCell

## Location
src/bin/psql/startup.c: 53 - 58

## Overview
A structure representing a single cell in a linked list that stores command-line actions to be executed by psql.

## Definition


## Detailed Description
SimpleActionListCell is a node structure used to build a linked list of actions that psql needs to perform based on command-line arguments. Each cell contains information about one action (such as executing a single query, processing a slash command, or reading from a file) along with any associated value. This structure is part of psql's startup mechanism that processes command-line options and queues them for sequential execution.

## Parameters / Member Variables
- `next`: Pointer to the next cell in the linked list, NULL for the last cell
- `action`: Enumerated value indicating the type of action to perform (ACT_SINGLE_QUERY, ACT_SINGLE_SLASH, or ACT_FILE)  
- `val`: String value associated with the action (e.g., SQL query text, file path), can be NULL if no value is needed

## Dependencies
- Functions called/Symbols referenced:
  - _actions (enum type for action field)
- Called from (representative examples):
  - simple_action_list_append (allocates and initializes cells)
  - SimpleActionList (used as linked list nodes)

## Notes and Other Information
- Memory for SimpleActionListCell instances is allocated using pg_malloc_object()
- The val field is duplicated using pg_strdup() when non-NULL to ensure the cell owns its string data
- This structure is part of psql's internal command processing and is not exposed to external modules
- The linked list structure allows psql to queue multiple actions from command-line arguments and execute them in order