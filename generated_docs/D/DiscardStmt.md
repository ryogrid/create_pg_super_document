# DiscardStmt

## Location
[src/include/nodes/parsenodes.h:3932-3936](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3932-L3936)

## Overview
DiscardStmt represents a DISCARD statement in PostgreSQL's parse tree, which is used to discard session state information like cached plans, temporary tables, sequences, or all session state.

## Definition

```c
typedef struct DiscardStmt
{
	NodeTag		type;
	DiscardMode target;
} DiscardStmt;
```
## Detailed Description
DiscardStmt is a parse tree node that represents the DISCARD SQL command. The DISCARD command allows users to discard various types of session state to free up memory or reset session-specific configurations. It supports four different targets:
- DISCARD ALL: Discards all session state (plans, sequences, temp tables, etc.)
- DISCARD PLANS: Discards cached prepared statement plans
- DISCARD SEQUENCES: Discards cached sequence values
- DISCARD TEMP: Discards temporary table namespace

The structure is minimal, containing only the standard NodeTag for parse tree identification and a DiscardMode enum to specify what should be discarded.

## Parameters / Member Variables
- `type`: Standard NodeTag identifying this as a DiscardStmt node in the parse tree
- `target`: DiscardMode enum value specifying what to discard (ALL, PLANS, SEQUENCES, or TEMP)

## Dependencies
- Functions called/Symbols referenced:
  - DiscardMode (enum defining discard targets)
  
- Called from (representative examples):
  - DiscardCommand (main execution function in discard.c:31)
  - standard_ProcessUtility (utility command processor in utility.c:886)
  - CreateCommandTag (command tag creation in utility.c:2922)

## Notes and Other Information
- The DISCARD command is primarily used for memory management and session cleanup
- DISCARD ALL is particularly useful in connection pooling scenarios to reset session state
- The actual execution logic is handled by DiscardCommand() in src/backend/commands/discard.c
- DISCARD ALL includes additional safeguards like preventing execution within transaction blocks
- This is a utility statement that doesn't return any data, only performs cleanup operations