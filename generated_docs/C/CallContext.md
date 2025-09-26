# CallContext

## Location
[src/include/nodes/parsenodes.h:3513-3519](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3513-L3519)

## Overview
CallContext is a node structure that provides execution context information for procedure calls in PostgreSQL. It conveys transaction control behavior for procedural language handlers.

## Definition
```c
typedef struct CallContext
{
    pg_node_attr(nodetag_only)  /* this is not a member of parse trees */

    NodeTag     type;
    bool        atomic;
} CallContext;
```

## Detailed Description
CallContext is a minimal structure used to pass execution context information to procedural language handlers when executing stored procedures via CALL statements. The structure is marked with pg_node_attr(nodetag_only) indicating it's not part of normal parse trees but serves as a runtime context object. The primary purpose is to communicate whether the procedure call should execute in an atomic context, which affects transaction control capabilities within the procedure.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a CallContext node
- `atomic`: Boolean flag indicating whether the procedure executes in an atomic context (affects transaction control operations)

## Dependencies
- Functions called/Symbols referenced:
  - (None directly referenced)
- Called from (representative examples):
  - ExecuteCallStmt (src/backend/commands/functioncmds.c:2197, 2213)
  - plperl_func_handler (src/pl/plperl/plperl.c:2412, 2413)
  - plpython3_call_handler (src/pl/plpython/plpy_main.c:201, 202)
  - pltcl_func_handler (src/pl/tcl/pltcl.c:809, 810)

## Dependencies
- Functions called/Symbols referenced:
  - (None directly referenced)
- Called from (representative examples):
  - ExecuteCallStmt (src/backend/commands/functioncmds.c:2197, 2213)
  - plperl_func_handler (src/pl/plperl/plperl.c:2412, 2413)
  - plpython3_call_handler (src/pl/plpython/plpy_main.c:201, 202)
  - pltcl_func_handler (src/pl/tcl/pltcl.c:809, 810)

## Notes and Other Information
CallContext is created by ExecuteCallStmt and passed to procedural language handlers to indicate the execution environment. When atomic is true, the procedure cannot perform transaction control operations (like COMMIT or ROLLBACK). When false, the procedure can control transactions. This distinction is important for procedures that need to perform complex transaction management versus those that should execute as part of the current transaction.