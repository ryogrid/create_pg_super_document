# CallStmt

## Location
[src/include/nodes/parsenodes.h:3502-3511](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3502-L3511)

## Overview
CallStmt is a node structure representing an SQL CALL statement in PostgreSQL's parse tree. It encapsulates both the original function call and the transformed execution information for calling stored procedures.

## Definition
```c
typedef struct CallStmt
{
    NodeTag     type;
    /* from the parser */
    FuncCall   *funccall pg_node_attr(query_jumble_ignore);
    /* transformed call, with only input args */
    FuncExpr   *funcexpr;
    /* transformed output-argument expressions */
    List       *outargs;
} CallStmt;
```

## Detailed Description
CallStmt represents both the parsed and transformed forms of a CALL statement used to invoke stored procedures. The structure maintains the original parsed function call and creates a transformed version that separates input and output arguments. OUT-mode arguments are removed from the transformed funcexpr and stored separately in the outargs list. This design allows the system to handle procedure calls with output parameters correctly, providing a reference for result assignment while keeping the execution expression clean.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a CallStmt node
- `funccall`: Original FuncCall from the parser (marked to ignore in query jumbling)
- `funcexpr`: Transformed FuncExpr containing only input arguments for execution
- `outargs`: List of expressions representing all output arguments in declared order (used as assignment reference, not evaluated)

## Dependencies
- Functions called/Symbols referenced:
  - FuncCall
  - FuncExpr
- Called from (representative examples):
  - ExecuteCallStmt (src/backend/commands/functioncmds.c:2188)
  - CallStmtResultDesc (src/backend/commands/functioncmds.c:2365)
  - transformCallStmt (src/backend/parser/analyze.c:3088)
  - standard_ProcessUtility (src/backend/tcop/utility.c:851)

## Notes and Other Information
The CALL statement is used to invoke stored procedures (not functions) and can handle procedures with output parameters. The separation of input and output arguments allows PostgreSQL to execute the procedure with only the necessary input parameters while maintaining metadata about expected outputs. The query jumbling annotation on funccall indicates that the original call structure should be ignored when computing query fingerprints, focusing only on the transformed execution form.