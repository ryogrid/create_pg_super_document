# ACT_FILE

## Location
[src/bin/psql/startup.c:50-52](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L50-L52)

## Overview
ACT_FILE is an enumeration value that represents a file execution action in psql's command-line option processing system.

## Definition

```c
typedef struct SimpleActionListCell
{
	struct SimpleActionListCell *next;
	enum _actions action;
	char	   *val;
} SimpleActionListCell;
```
## Detailed Description
ACT_FILE is one of the action types defined in psql's startup module that specifies the type of operation to be performed. When ACT_FILE is used, it indicates that psql should execute SQL commands from a specified file. This enumeration value is part of the action list system that allows psql to queue multiple operations for execution in sequence.

The ACT_FILE action is typically triggered by the  command-line option, which tells psql to read and execute commands from a file rather than from standard input or interactive mode. It can also be automatically assigned when psql is running in non-interactive mode without any explicit actions specified.

## Parameters / Member Variables
- `ACT_FILE`: Enumeration constant indicating file execution action (no additional parameters in the enum itself)

## Dependencies
- Functions called/Symbols referenced:
  - Used within  enumeration
- Called from (representative examples):
  -  (startup.c:216) - Auto-assigned for non-interactive mode
  -  (startup.c:566) - Assigned when  option is processed
  - Action processing logic (startup.c:413) - Used in conditional checks during execution

## Notes and Other Information
- [ACT_FILE](ACT_FILE.md) is used in conjunction with the SimpleActionListCell structure to create a linked list of actions
- When ACT_FILE is processed, the associated  field in SimpleActionListCell contains the filename to be executed
- In non-interactive mode, if no actions are specified, ACT_FILE with NULL filename is automatically added to execute stdin
- The action is processed by calling  function with the specified filename
- Part of psql's command-line argument processing and execution flow control system