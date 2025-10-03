# transformJsonTable

## Location
[src/backend/parser/parse_jsontable.c:76-172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_jsontable.c#L76-L172)

## Overview
Transforms a raw JsonTable node into a TableFunc node, handling the transformation of JSON_TABLE expressions and column specifications for PostgreSQL's JSON table functionality.

## Definition

```c
ParseNamespaceItem *
transformJsonTable(ParseState *pstate, JsonTable *jt)
```
## Detailed Description
The transformJsonTable function is the main entry point for processing JSON_TABLE expressions in PostgreSQL. It converts a raw JsonTable AST node into a TableFunc node that can be executed by the PostgreSQL execution engine. The function performs several key operations:

1. Validates the ON ERROR behavior clause, ensuring only valid behaviors (ERROR, EMPTY, or EMPTY ARRAY) are specified
2. Generates path names for unnamed path specifications and checks for duplicate column/path names
3. Transforms the context item expression and path specification into a JsonFuncExpr with JSON_TABLE_OP operation
4. Creates a JsonTablePlan by transforming column specifications and their associated JSON path expressions
5. Sets up lateral reference handling and creates the appropriate range table entry

The function ensures SQL standard compliance and proper handling of lateral references within the JSON_TABLE construct.

## Parameters / Member Variables
- `*pstate`: ParseState context containing parsing information and namespace
- `*jt`: JsonTable AST node containing the raw JSON_TABLE specification including context item, path specification, columns, and error handling clauses
## Dependencies
- Functions called/Symbols referenced:
  - [generateJsonTablePathName](../g/generateJsonTablePathName.md)
  - [CheckDuplicateColumnOrPathNames](../C/CheckDuplicateColumnOrPathNames.md)
  - makeNode
  - [transformExpr](transformExpr.md)
  - [transformJsonTableColumns](transformJsonTableColumns.md)
  - copyObject
  - [contain_vars_of_level](../c/contain_vars_of_level.md)
  - [addRangeTableEntryForTableFunc](../a/addRangeTableEntryForTableFunc.md)
- Called from (representative examples):
  - [transformFromClauseItem](transformFromClauseItem.md)

## Notes and Other Information
- The function temporarily enables lateral reference resolution (p_lateral_active = true) during transformation to comply with SQL specification requirements
- Only specific ON ERROR behaviors are allowed at the top level: ERROR, EMPTY, or EMPTY ARRAY
- The function creates a dummy JSON_TABLE_OP JsonExpr to represent the top-level context item and path specification
- PASSING arguments are duplicated in both the JsonExpr and TableFunc nodes for separate evaluation contexts
- The resulting ParseNamespaceItem is marked as lateral if explicitly specified or if lateral cross-references are detected

## Simplified Source

```c
ParseNamespaceItem *transformJsonTable(ParseState *pstate, JsonTable *jt)
{
    TableFunc *tf;
    JsonFuncExpr *jfe;
    JsonExpr *je;
    JsonTablePathSpec *rootPathSpec = jt->pathspec;
    bool is_lateral;
    JsonTableParseContext cxt = {pstate};

    // Validate ON ERROR behavior - only ERROR, EMPTY, or EMPTY ARRAY allowed
    if (jt->on_error &&
        jt->on_error->btype != JSON_BEHAVIOR_ERROR &&
        jt->on_error->btype != JSON_BEHAVIOR_EMPTY &&
        jt->on_error->btype != JSON_BEHAVIOR_EMPTY_ARRAY)
        ereport(ERROR, "invalid ON ERROR behavior");

    // Generate path names and check for duplicates
    cxt.pathNameId = 0;
    if (rootPathSpec->name == NULL)
        rootPathSpec->name = generateJsonTablePathName(&cxt);
    cxt.pathNames = list_make1(rootPathSpec->name);
    CheckDuplicateColumnOrPathNames(&cxt, jt->columns);

    // Enable lateral references for SQL spec compliance
    pstate->p_lateral_active = true;

    // Create TableFunc node
    tf = makeNode(TableFunc);
    tf->functype = TFT_JSON_TABLE;

    // Transform context item and pathspec into JsonExpr
    jfe = makeNode(JsonFuncExpr);
    jfe->op = JSON_TABLE_OP;
    jfe->context_item = jt->context_item;
    jfe->pathspec = (Node *) rootPathSpec->string;
    jfe->passing = jt->passing;
    jfe->on_error = jt->on_error;
    jfe->location = jt->location;
    tf->docexpr = transformExpr(pstate, (Node *) jfe, EXPR_KIND_FROM_FUNCTION);

    // Transform column specifications into execution plan
    cxt.jt = jt;
    cxt.tf = tf;
    tf->plan = (Node *) transformJsonTableColumns(&cxt, jt->columns,
                                                  jt->passing, rootPathSpec);

    // Copy PASSING arguments for separate evaluation
    je = (JsonExpr *) tf->docexpr;
    tf->passingvalexprs = copyObject(je->passing_values);

    tf->ordinalitycol = -1;  // No ordinality column
    tf->location = jt->location;

    pstate->p_lateral_active = false;

    // Determine if LATERAL marking is needed
    is_lateral = jt->lateral || contain_vars_of_level((Node *) tf, 0);

    return addRangeTableEntryForTableFunc(pstate, tf, jt->alias, is_lateral, true);
}
```