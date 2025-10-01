# pg_get_expr_worker

## Location
[src/backend/utils/adt/ruleutils.c:2664-2748](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L2664-L2748)

## Overview
Core internal function that performs the actual conversion of stored pg_node_tree expressions back into human-readable SQL text format with validation and context handling.

## Definition
```c
static text *pg_get_expr_worker(text *expr, Oid relid, int prettyFlags)
```

## Detailed Description
pg_get_expr_worker is the internal workhorse function responsible for converting PostgreSQL's stored expression trees back into readable SQL text. It handles the complex process of deserializing a pg_node_tree (stored as TEXT), validating the expression structure, checking variable references, setting up deparse context, and finally converting the node tree back to SQL text format.

The function performs several important validation steps: it ensures the input is an expression (not a query), validates that variable references are consistent with the provided relation context, and handles relation locking to ensure consistent access to metadata during deparsing. If a relation OID is provided, it opens the relation to provide proper context for column name resolution.

## Parameters / Member Variables
- `expr`: TEXT containing the serialized pg_node_tree expression to be converted
- `relid`: OID of the relation providing context for variable resolution (InvalidOid if no relation context)
- `prettyFlags`: Integer flags controlling output formatting options (indentation, line breaks, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [text_to_cstring](../t/text_to_cstring.md) (converts TEXT to C string)
  - [stringToNode](../s/stringToNode.md) (deserializes string to Node tree)
  - [pull_varnos](pull_varnos.md) (extracts variable range table indices)
  - [bms_make_singleton](../b/bms_make_singleton.md) (creates single-element bitmap set)
  - [bms_is_subset](../b/bms_is_subset.md) (checks bitmap subset relationship)
  - bms_is_empty (checks if bitmap is empty)
  - [try_relation_open](../t/try_relation_open.md) (attempts to open relation with lock)
  - [deparse_context_for](../d/deparse_context_for.md) (creates deparse context for relation)
  - [deparse_expression_pretty](../d/deparse_expression_pretty.md) (converts node tree to formatted SQL)
  - [relation_close](../r/relation_close.md) (closes relation and releases lock)
  - [string_to_text](../s/string_to_text.md) (converts C string to TEXT)
- Called from:
  - [pg_get_expr](pg_get_expr.md) (standard version without pretty-printing)
  - [pg_get_expr_ext](pg_get_expr_ext.md) (extended version with pretty-printing)

## Notes and Other Information
- This is a static function, not directly callable from SQL
- Returns NULL if the relation cannot be opened or accessed
- Temporarily locks relations during deparsing to ensure consistency
- Performs extensive validation to prevent errors during deparsing
- Handles both relation-contextualized expressions and standalone expressions
- Located in src/backend/utils/adt/ruleutils.c:2664-2748
- Uses AccessShareLock when opening relations to prevent concurrent modifications

## Simplified Source

```c
static text *pg_get_expr_worker(text *expr, Oid relid, int prettyFlags) {
    Node *node;
    Relids relids;
    List *context;
    char *exprstr;
    Relation rel = NULL;
    char *str;

    // Convert TEXT input to C string and parse into node tree
    exprstr = text_to_cstring(expr);
    node = (Node *) stringToNode(exprstr);
    pfree(exprstr);

    // Validate input is an expression, not a query
    Node *tst = node;
    while (tst && IsA(tst, List))
        tst = linitial((List *) tst);
    if (tst && IsA(tst, Query))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("input is a query, not an expression")));

    // Validate variable references are consistent with relation context
    relids = pull_varnos(NULL, node);
    if (OidIsValid(relid)) {
        if (!bms_is_subset(relids, bms_make_singleton(1)))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("expression contains variables of more than one relation")));
    } else {
        if (!bms_is_empty(relids))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("expression contains variables")));
    }

    // Set up deparse context with relation info if needed
    if (OidIsValid(relid)) {
        rel = try_relation_open(relid, AccessShareLock);
        if (rel == NULL)
            return NULL;
        context = deparse_context_for(RelationGetRelationName(rel), relid);
    } else {
        context = NIL;
    }

    // Convert node tree back to formatted SQL text
    str = deparse_expression_pretty(node, context, false, false, prettyFlags, 0);

    // Clean up relation lock
    if (rel != NULL)
        relation_close(rel, AccessShareLock);

    return string_to_text(str);
}
```