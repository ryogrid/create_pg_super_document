# processIndirection

## Location
[src/backend/utils/adt/ruleutils.c:12591-12668](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L12591-L12668)

## Overview
Processes array and subfield assignment indirection by stripping top-level FieldStore or assignment SubscriptingRef nodes and printing them as decoration for the base column name.

## Definition
```c
static Node *processIndirection(Node *node, deparse_context *context)
```

## Detailed Description
This function handles the decompilation of complex assignment expressions involving field access and array subscripting in PostgreSQL rules. It iteratively processes and strips top-level FieldStore nodes (for field assignments) and SubscriptingRef nodes (for array element assignments) from the input expression tree, while generating the appropriate SQL syntax decoration (like .fieldname or [subscripts]) that represents the indirection operations.

The function also handles CoerceToDomain nodes, but only those that appear above assignment nodes and represent implicit casts. It carefully preserves explicit domain coercions by stopping traversal when encountering them.

The core logic involves a loop that examines each node type and either processes it (generating output) and continues deeper, or breaks when encountering a node that should not be processed further.

## Parameters / Member Variables
- `node`: The expression node to process, typically containing nested indirection operations
- `context`: The deparse context containing the output buffer and other decompilation state

## Dependencies
- Functions called/Symbols referenced:
  - [get_typ_typrelid](../g/get_typ_typrelid.md) (retrieves type relation ID for tuple types)
  - [get_attname](../g/get_attname.md) (gets attribute name from relation and attribute number)
  - [quote_identifier](../q/quote_identifier.md) (properly quotes field names)
  - [printSubscripts](printSubscripts.md) (handles array subscript printing)
  - linitial_int (gets first integer from list)
- Called from (representative examples):
  - [get_insert_query_def](../g/get_insert_query_def.md)
  - [get_update_query_targetlist_def](../g/get_update_query_targetlist_def.md)
  - [get_merge_query_def](../g/get_merge_query_def.md)
  - [get_rule_expr](../g/get_rule_expr.md)

## Notes and Other Information
- Only processes FieldStore nodes with exactly one target field, as expected in stored rules
- Ignores the arg/refexpr components since they should be uninteresting references to target columns
- Handles nested CoerceToDomain nodes with care to preserve explicit domain coercions
- Returns the final subexpression that represents the value to be assigned
- Critical component of PostgreSQL's rule system for decompiling complex assignment expressions
- The function assumes that the caller has already printed the base column name

## Simplified Source

```c
static Node *
processIndirection(Node *node, deparse_context *context)
{
    StringInfo buf = context->buf;
    CoerceToDomain *cdomain = NULL;

    // Process assignment operations by stripping off field stores and subscripts
    for (;;)
    {
        if (node == NULL)
            break;

        if (IsA(node, FieldStore))
        {
            FieldStore *fstore = (FieldStore *) node;

            // Get field name and append to output
            Oid typrelid = get_typ_typrelid(fstore->resulttype);
            char *fieldname = get_attname(typrelid, linitial_int(fstore->fieldnums), false);
            appendStringInfo(buf, ".%s", quote_identifier(fieldname));

            // Move to the new value being assigned
            node = (Node *) linitial(fstore->newvals);
        }
        else if (IsA(node, SubscriptingRef))
        {
            SubscriptingRef *sbsref = (SubscriptingRef *) node;

            if (sbsref->refassgnexpr == NULL)
                break;

            // Print array subscripts
            printSubscripts(sbsref, context);

            // Move to the assignment expression
            node = (Node *) sbsref->refassgnexpr;
        }
        else if (IsA(node, CoerceToDomain))
        {
            cdomain = (CoerceToDomain *) node;

            // Stop if explicit domain coercion
            if (cdomain->coercionformat != COERCE_IMPLICIT_CAST)
                break;

            // Continue past implicit coercion
            node = (Node *) cdomain->arg;
        }
        else
            break;
    }

    // Handle case where we stepped past a coercion incorrectly
    if (cdomain && node == (Node *) cdomain->arg)
        node = (Node *) cdomain;

    return node;
}
```