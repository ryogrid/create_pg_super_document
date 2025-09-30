# DefineView

## Location
[src/backend/commands/view.c:356-510](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/view.c#L356-L510)

## Overview
DefineView executes a CREATE VIEW command, performing parsing, validation, and view creation by coordinating the entire view definition process from SQL statement to database object.

## Definition

```c
ObjectAddress
DefineView(ViewStmt *stmt, const char *queryString,
		   int stmt_location, int stmt_len)
```
## Detailed Description
DefineView is the main entry point for CREATE VIEW and CREATE OR REPLACE VIEW commands. It orchestrates the complete process of view creation through several key phases:

1. **Parse Analysis**: Converts the raw parse tree to a fully analyzed Query node, acquiring necessary locks on source tables in the process.

2. **Validation**: Performs comprehensive validation including:
   - Ensuring the result is a single SELECT query
   - Rejecting SELECT INTO constructs
   - Checking for unsupported features like data-modifying CTEs
   - Validating WITH CHECK OPTION requirements

3. **Option Processing**: Handles view options, particularly converting WITH CHECK OPTION specifications to internal option representations and validating that CHECK OPTION is only used with auto-updatable views.

4. **Column Alias Assignment**: If column aliases were specified in the CREATE VIEW statement, assigns them to the corresponding target list entries.

5. **Persistence Handling**: Manages view persistence settings, automatically converting views to temporary if they reference temporary relations (with user notification).

6. **View Creation**: Delegates the actual view relation creation to DefineVirtualRelation.

The function performs extensive error checking and provides helpful error messages for various edge cases and unsupported scenarios.

## Parameters / Member Variables
- : ViewStmt node containing the parsed CREATE VIEW statement structure
- : Original SQL query string for error reporting and logging
- : Character offset where the statement begins in the query string
- : Length of the statement in characters

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for RawStmt creation)
  - [parse_analyze_fixedparams](../p/parse_analyze_fixedparams.md)
  - IsA (type checking macros)
  - elog, ereport (error reporting)
  - [makeDefElem](../m/makeDefElem.md), makeString (option creation)
  - [view_query_is_auto_updatable](../v/view_query_is_auto_updatable.md)
  - [list_head](../l/list_head.md), lnext, lfirst_node (list manipulation)
  - [pstrdup](../p/pstrdup.md), strVal (string utilities)
  - copyObject
  - [isQueryUsingTempRelation](../i/isQueryUsingTempRelation.md)
  - [DefineVirtualRelation](DefineVirtualRelation.md)

- Called from:
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (main utility command processor)

## Notes and Other Information
- The function automatically promotes views to temporary status if they reference temporary relations, issuing a NOTICE to inform the user
- WITH CHECK OPTION validation is performed early to provide clear error messages specific to views rather than generic rule system errors  
- Column alias assignment supports partial alias lists (fewer aliases than columns) but rejects lists with too many aliases
- Unlogged views are explicitly rejected since views have no storage
- The function preserves the original statement structure by copying it before modifications
- Parse analysis occurs early to ensure proper lock acquisition on referenced tables
- Error messages are designed to be view-specific rather than exposing internal rule system details

## Simplified Source

```c
ObjectAddress
DefineView(ViewStmt *stmt, const char *queryString,
           int stmt_location, int stmt_len)
{
    RawStmt *rawstmt;
    Query *viewParse;
    RangeVar *view;
    ObjectAddress address;

    // Parse the view query
    rawstmt = makeNode(RawStmt);
    rawstmt->stmt = stmt->query;
    rawstmt->stmt_location = stmt_location;
    rawstmt->stmt_len = stmt_len;

    viewParse = parse_analyze_fixedparams(rawstmt, queryString, NULL, 0, NULL);

    // Validate query type and restrictions
    if (!IsA(viewParse, Query))
        elog(ERROR, "unexpected parse analysis result");

    if (viewParse->commandType != CMD_SELECT)
        elog(ERROR, "unexpected parse analysis result");

    if (viewParse->hasModifyingCTE)
        ereport(ERROR, "views must not contain data-modifying statements in WITH");

    // Handle WITH CHECK OPTION
    if (stmt->withCheckOption == LOCAL_CHECK_OPTION)
        stmt->options = lappend(stmt->options,
                               makeDefElem("check_option", makeString("local"), -1));
    else if (stmt->withCheckOption == CASCADED_CHECK_OPTION)
        stmt->options = lappend(stmt->options,
                               makeDefElem("check_option", makeString("cascaded"), -1));

    // Validate auto-updatable if CHECK OPTION specified
    if (check_option_specified(stmt->options)) {
        const char *view_updatable_error = view_query_is_auto_updatable(viewParse, true);
        if (view_updatable_error)
            ereport(ERROR, "WITH CHECK OPTION is supported only on automatically updatable views");
    }

    // Assign column aliases if provided
    if (stmt->aliases != NIL) {
        assign_column_aliases(viewParse->targetList, stmt->aliases);
    }

    // Handle temporary view promotion
    view = copyObject(stmt->view);
    if (view->relpersistence == RELPERSISTENCE_PERMANENT &&
        isQueryUsingTempRelation(viewParse)) {
        view->relpersistence = RELPERSISTENCE_TEMP;
        ereport(NOTICE, "view will be a temporary view");
    }

    // Create the view relation
    address = DefineVirtualRelation(view, viewParse->targetList,
                                   stmt->replace, stmt->options, viewParse);

    return address;
}
```