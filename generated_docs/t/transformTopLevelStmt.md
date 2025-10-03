# transformTopLevelStmt

## Location
[src/backend/parser/analyze.c:248-271](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/analyze.c#L248-L271)

## Overview
Transforms a raw parse tree into a Query tree, handling top-level statement processing including SELECT INTO operations and statement location tracking.

## Definition

```c
Query *
transformTopLevelStmt(ParseState *pstate, RawStmt *parseTree)
```
## Detailed Description
This function serves as the entry point for transforming top-level SQL statements from their raw parsed form into fully analyzed Query structures. Despite its central role in the parsing pipeline, the function itself is relatively simple and primarily acts as a coordinator.

The main responsibilities include:
1. Delegating the actual transformation work to transformOptionalSelectInto
2. Ensuring that SELECT INTO operations are allowed at the top level
3. Transferring statement location information from the RawStmt to the resulting Query
4. Setting up the Query structure with proper position tracking for error reporting

The function is specifically designed to handle top-level statements, which have different rules compared to sub-statements (particularly regarding SELECT INTO operations).

## Parameters / Member Variables
- `*pstate`: Parse state containing context and configuration for the transformation
- `*parseTree`: Raw statement structure containing the parsed SQL along with location information
## Dependencies
- Functions called/Symbols referenced:
  - [transformOptionalSelectInto](transformOptionalSelectInto.md): Performs the actual statement transformation with SELECT INTO handling
  - [RawStmt](../R/RawStmt.md): Structure containing the raw parsed statement and location data

- Called from (representative examples):
  - [parse_analyze_fixedparams](../p/parse_analyze_fixedparams.md): Top-level analysis with fixed parameters
  - [parse_analyze_varparams](../p/parse_analyze_varparams.md): Top-level analysis with variable parameters
  - [parse_analyze_withcb](../p/parse_analyze_withcb.md): Top-level analysis with custom callback
  - [inline_function](../i/inline_function.md): Used during query optimization for function inlining

## Notes and Other Information
- This function specifically handles top-level statements, enabling SELECT INTO operations
- The main transformation logic is delegated to transformOptionalSelectInto
- Statement location tracking (stmt_location and stmt_len) is essential for error reporting
- Unlike transformStmt, this function allows SELECT INTO operations at the top level
- The function is a thin wrapper that primarily handles location data transfer
- Part of the parsing pipeline that connects raw parsing with semantic analysis
- Used exclusively for top-level statements, not for sub-statements or recursive analysis

## Simplified Source

```c
// Simplified version of transformTopLevelStmt
Query *transformTopLevelStmt(ParseState *pstate, RawStmt *parseTree) {
    Query *result;

    // Transform statement allowing SELECT INTO at top level
    result = transformOptionalSelectInto(pstate, parseTree->stmt);

    // Transfer location information for error reporting
    result->stmt_location = parseTree->stmt_location;
    result->stmt_len = parseTree->stmt_len;

    return result;
}
```

Key simplifications made:
- Preserved essential statement transformation delegation
- Maintained location information transfer
- Focused on core top-level processing functionality
- Kept SELECT INTO handling capability