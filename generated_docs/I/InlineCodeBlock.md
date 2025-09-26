# InlineCodeBlock

## Location
src/include/nodes/parsenodes.h: 3480 - 3489

## Overview
InlineCodeBlock is a node structure representing the execution-time information for DO statements in PostgreSQL. It contains the processed code block ready for execution by a procedural language handler.

## Definition
```c
typedef struct InlineCodeBlock
{
    pg_node_attr(nodetag_only)  /* this is not a member of parse trees */

    NodeTag     type;
    char       *source_text;    /* source text of anonymous code block */
    Oid         langOid;        /* OID of selected language */
    bool        langIsTrusted;  /* trusted property of the language */
    bool        atomic;         /* atomic execution context */
} InlineCodeBlock;
```

## Detailed Description
InlineCodeBlock serves as the execution-time API for DO statements, as opposed to DoStmt which is the raw parser output. This structure contains all the information needed to execute an anonymous code block, including the source code, language identification, and execution context properties. The structure is marked with pg_node_attr(nodetag_only) indicating it's not part of normal parse trees but is used during execution.

## Parameters / Member Variables
- `type`: NodeTag identifying this as an InlineCodeBlock node
- `source_text`: The actual source code text of the anonymous code block to be executed
- `langOid`: Object identifier (OID) of the procedural language that will execute this code
- `langIsTrusted`: Boolean flag indicating whether the language is trusted (affects security permissions)
- `atomic`: Boolean flag indicating whether the code block executes in an atomic context (affects transaction behavior)

## Dependencies
- Functions called/Symbols referenced:
  - (None directly referenced)
- Called from (representative examples):
  - ExecuteDoStmt (src/backend/commands/functioncmds.c:2068)
  - plperl_inline_handler (src/pl/plperl/plperl.c:1897)
  - plpython3_inline_handler (src/pl/plpython/plpy_main.c:266)

## Notes and Other Information
This structure is created by ExecuteDoStmt when processing a DoStmt and is passed to the appropriate procedural language handler for execution. Each procedural language (PL/pgSQL, PL/Perl, PL/Python, etc.) provides an inline handler function that receives an InlineCodeBlock and executes the contained code. The atomic flag controls whether the code block can perform transaction control operations.