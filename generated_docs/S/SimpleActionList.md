# SimpleActionList

## Location
[src/bin/psql/startup.c:60-64](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L60-L64)

## Overview
A linked list structure that maintains a queue of command-line actions to be executed by psql in sequence.

## Definition

```c
typedef struct SimpleActionList
{
	SimpleActionListCell *head;
	SimpleActionListCell *tail;
} SimpleActionList;
```
## Detailed Description
SimpleActionList is a simple linked list implementation that stores a sequence of actions parsed from psql command-line arguments. The structure maintains both head and tail pointers for efficient appending of new actions to the end of the list. Actions are processed sequentially in the order they were specified on the command line, allowing psql to execute multiple queries, slash commands, or file inputs in a single invocation.

## Parameters / Member Variables
- `head`: Pointer to the first SimpleActionListCell in the list, NULL if the list is empty
- `tail`: Pointer to the last SimpleActionListCell in the list, used for efficient O(1) append operations, NULL if the list is empty

## Dependencies
- Functions called/Symbols referenced:
  - [SimpleActionListCell](SimpleActionListCell.md) (node type for the linked list)
- Called from (representative examples):
  - [simple_action_list_append](../s/simple_action_list_append.md) (adds new actions to the list)
  - [adhoc_opts](../a/adhoc_opts.md) (contains an instance for storing parsed command-line actions)
  - [main](../m/main.md) function (iterates through actions.head to process each queued action)

## Notes and Other Information
- The list is processed in main() by iterating from head to tail, executing each action in sequence
- Actions are appended using simple_action_list_append() which maintains both head and tail pointers
- Used specifically in psql's startup process to handle command-line options like -c (single query) and -f (file input)
- The structure enables psql to queue multiple actions from the command line and execute them after database connection is established
- Empty list (head == NULL) indicates interactive mode should be started if no TTY is available