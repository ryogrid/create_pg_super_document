# DoStmt

## Location
[src/include/nodes/parsenodes.h:3474-3478](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3474-L3478)

## Overview
DoStmt is a node structure representing an SQL DO statement in PostgreSQL's parse tree. It serves as the raw parser output for DO statements that execute anonymous code blocks in procedural languages.

## Definition
```c
typedef struct DoStmt
{
    NodeTag     type;
    List       *args;           /* List of DefElem nodes */
} DoStmt;
```

## Detailed Description
DoStmt represents the parsed form of a DO statement, which allows execution of anonymous procedural language code blocks. The structure captures the language and code text specified in the DO statement. As noted in the source comments, DoStmt is the raw parser output, while InlineCodeBlock is used during execution time. The args list typically contains DefElem nodes specifying the language and the code text to be executed.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a DoStmt node
- `args`: List of DefElem structures containing the DO statement parameters (typically language specification and code text)

## Dependencies
- Functions called/Symbols referenced:
  - (None directly referenced)
- Called from (representative examples):
  - ExecuteDoStmt (src/backend/commands/functioncmds.c:2066)
  - standard_ProcessUtility (src/backend/tcop/utility.c:707)

## Notes and Other Information
The DO statement allows execution of procedural language code without creating a persistent function. DoStmt is processed during utility command execution and is converted to an InlineCodeBlock structure for actual execution. This provides a convenient way to run one-time procedural code blocks in languages like PL/pgSQL, PL/Python, etc.