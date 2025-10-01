# get_setop_query

## Location
[src/backend/utils/adt/ruleutils.c:6171-6318](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L6171-L6318)

## Overview
Recursively generates SQL text for set operation queries (UNION, INTERSECT, EXCEPT) by processing SetOperationStmt nodes and their operands.

## Definition
```c
static void get_setop_query(Node *setOp, Query *query, deparse_context *context)
```

## Detailed Description
This recursive function converts set operation parse trees back into SQL text format. It handles two main node types:

1. **RangeTblRef nodes**: Represents leaf queries in the set operation tree. The function determines when parentheses are needed based on the presence of WITH, ORDER BY, FOR UPDATE, LIMIT clauses, or nested set operations.

2. **SetOperationStmt nodes**: Represents internal nodes (UNION, INTERSECT, EXCEPT operations). The function applies intelligent parenthesization rules to minimize unnecessary parentheses while ensuring correct precedence. It avoids parentheses when the left operand is the same type of set operation.

Key features:
- Recursive processing of nested set operations
- Intelligent parenthesization based on SQL grammar rules
- Proper indentation and formatting for complex nested queries
- Handling of ALL modifier for set operations
- Stack depth checking to prevent infinite recursion
- Suppression of column names for right-hand operands (not relevant for output)

## Parameters / Member Variables
- `setOp`: Node representing either a RangeTblRef (leaf query) or SetOperationStmt (set operation)
- `query`: The parent Query structure containing the range table
- `context`: deparse_context containing formatting options and output buffer

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md) (prevent stack overflow in recursive calls)
  - rt_fetch (retrieve range table entry by index)
  - [get_query_def](get_query_def.md) (generate SQL for subqueries)
  - [appendContextKeyword](../a/appendContextKeyword.md) (format keywords with proper indentation)
  - nodeTag (get node type for error checking)
- Called from (representative examples):
  - [get_select_query_def](get_select_query_def.md) (src/backend/utils/adt/ruleutils.c:5722)
  - [get_setop_query](get_setop_query.md) (recursive calls at lines 6247 and 6297)

## Notes and Other Information
- Part of PostgreSQL's rule decompilation system for complex set operations
- Implements sophisticated parenthesization logic to produce clean, readable SQL
- Handles all three standard SQL set operations: UNION, INTERSECT, EXCEPT
- Supports both ALL and DISTINCT variants of set operations
- Manages proper indentation for deeply nested set operation trees
- Uses recursion with stack depth protection for safety
- Critical for view definition storage and rule system functionality

## Simplified Source

```c
static void
get_setop_query(Node *setOp, Query *query, deparse_context *context)
{
    StringInfo buf = context->buf;
    bool need_paren;

    // Prevent stack overflow and interruption
    CHECK_FOR_INTERRUPTS();
    check_stack_depth();

    if (IsA(setOp, RangeTblRef)) {
        // Handle leaf query (base table or subquery)
        RangeTblRef *rtr = (RangeTblRef *) setOp;
        RangeTblEntry *rte = rt_fetch(rtr->rtindex, query->rtable);
        Query *subquery = rte->subquery;

        Assert(subquery != NULL);

        // Need parentheses if query has WITH, ORDER BY, FOR UPDATE, LIMIT, or set operations
        need_paren = (subquery->cteList ||
                      subquery->sortClause ||
                      subquery->rowMarks ||
                      subquery->limitOffset ||
                      subquery->limitCount ||
                      subquery->setOperations);

        if (need_paren)
            appendStringInfoChar(buf, '(');

        get_query_def(subquery, buf, context->namespaces,
                      context->resultDesc, context->colNamesVisible,
                      context->prettyFlags, context->wrapColumn,
                      context->indentLevel);

        if (need_paren)
            appendStringInfoChar(buf, ')');
    }
    else if (IsA(setOp, SetOperationStmt)) {
        // Handle set operation (UNION, INTERSECT, EXCEPT)
        SetOperationStmt *op = (SetOperationStmt *) setOp;
        int subindent;
        bool save_colnamesvisible;

        // Determine if left operand needs parentheses
        // Avoid parens when left operand is same type of set operation
        if (IsA(op->larg, SetOperationStmt)) {
            SetOperationStmt *lop = (SetOperationStmt *) op->larg;
            need_paren = !(op->op == lop->op && op->all == lop->all);
        } else {
            need_paren = false;
        }

        // Process left operand with parentheses if needed
        if (need_paren) {
            appendStringInfoChar(buf, '(');
            subindent = PRETTYINDENT_STD;
            appendContextKeyword(context, "", subindent, 0, 0);
        } else {
            subindent = 0;
        }

        get_setop_query(op->larg, query, context);

        if (need_paren)
            appendContextKeyword(context, ") ", -subindent, 0, 0);
        else if (PRETTY_INDENT(context))
            appendContextKeyword(context, "", -subindent, 0, 0);
        else
            appendStringInfoChar(buf, ' ');

        // Add set operation keyword
        switch (op->op) {
            case SETOP_UNION:
                appendStringInfoString(buf, "UNION ");
                break;
            case SETOP_INTERSECT:
                appendStringInfoString(buf, "INTERSECT ");
                break;
            case SETOP_EXCEPT:
                appendStringInfoString(buf, "EXCEPT ");
                break;
            default:
                elog(ERROR, "unrecognized set op: %d", (int) op->op);
        }

        if (op->all)
            appendStringInfoString(buf, "ALL ");

        // Process right operand (always parenthesize if it's another setop)
        need_paren = IsA(op->rarg, SetOperationStmt);

        if (need_paren) {
            appendStringInfoChar(buf, '(');
            subindent = PRETTYINDENT_STD;
        } else {
            subindent = 0;
        }
        appendContextKeyword(context, "", subindent, 0, 0);

        // Hide column names for right operand
        save_colnamesvisible = context->colNamesVisible;
        context->colNamesVisible = false;

        get_setop_query(op->rarg, query, context);

        context->colNamesVisible = save_colnamesvisible;

        if (PRETTY_INDENT(context))
            context->indentLevel -= subindent;
        if (need_paren)
            appendContextKeyword(context, ")", 0, 0, 0);
    }
    else {
        elog(ERROR, "unrecognized node type: %d", (int) nodeTag(setOp));
    }
}
```