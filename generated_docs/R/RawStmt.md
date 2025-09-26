# RawStmt

## Location
src/include/nodes/parsenodes.h: 2017 - 2025

## Overview
RawStmt serves as a container for any single statement's raw parse tree, representing the initial output from the parser before analysis and transformation.

## Definition
```c
typedef struct RawStmt
{
    pg_node_attr(no_query_jumble)
    
    NodeTag     type;
    Node       *stmt;           /* raw parse tree */
    ParseLoc    stmt_location;  /* start location, or -1 if unknown */
    ParseLoc    stmt_len;       /* length in bytes; 0 means "rest of string" */
} RawStmt;
```

## Detailed Description
RawStmt acts as the top-level wrapper for raw parse trees produced by the SQL parser. It encapsulates any parsed statement along with positional information about where the statement appears in the source text. Parse analysis later converts statements headed by RawStmt nodes into analyzed statements headed by Query nodes. For optimizable statements (SELECT, INSERT, UPDATE, DELETE), this conversion involves complex analysis. For utility statements (CREATE, DROP, etc.), the parser typically just transfers the raw parse tree into the Query node's utilityStmt field, deferring most processing to execution time. The structure is marked to be excluded from query jumbling since it's not used in parsed queries.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a RawStmt node
- `stmt`: Pointer to the actual raw parse tree node representing the parsed statement
- `stmt_location`: Parse location indicating the start position of the statement in the source text (-1 if unknown)
- `stmt_len`: Length of the statement in bytes; 0 means the statement extends to the end of the string

## Dependencies
- Functions called/Symbols referenced:
  - ParseLoc
- Called from (representative examples):
  - parse_analyze_fixedparams
  - parse_analyze_varparams
  - transformTopLevelStmt
  - exec_simple_query
  - exec_parse_message
  - CreateCachedPlan

## Notes and Other Information
- Essential for multi-statement strings where precise statement boundaries need to be tracked
- The no_query_jumble attribute indicates this structure is not included in query fingerprinting
- Location information is crucial for accurate error reporting and debugging
- Serves as the bridge between raw parsing and semantic analysis phases
- Used extensively throughout the parser, planner, and execution subsystems
- The stmt_len field enables efficient substring operations without full string scanning