# make_ruledef

## Location
[src/backend/utils/adt/ruleutils.c:5160-5351](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L5160-L5351)

## Overview
A static function that reconstructs the CREATE RULE command text for a given pg_rewrite tuple.

## Definition

```c
static void
make_ruledef(StringInfo buf, HeapTuple ruletup, TupleDesc rulettc,
			 int prettyFlags)
```
## Detailed Description
The  function is responsible for reconstructing the complete CREATE RULE command from a PostgreSQL rule tuple stored in the pg_rewrite system catalog. This function extracts all rule attributes from the tuple and formats them into the standard SQL CREATE RULE syntax. It handles different event types (SELECT, UPDATE, INSERT, DELETE), optional qualifications (WHERE clauses), and rule actions. The function also handles formatting options and produces properly qualified object names.

The function processes rule metadata including the rule name, event type, target relation, qualification expressions, and action queries. It formats the output according to the specified pretty-printing flags and ensures proper quoting and qualification of identifiers.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the reconstructed CREATE RULE command will be written
- `ruletup`: HeapTuple containing the rule data from pg_rewrite catalog
- `rulettc`: TupleDesc describing the structure of the rule tuple
- `prettyFlags`: Integer flags controlling output formatting (indentation, schema qualification, etc.)
## Dependencies
- Functions called/Symbols referenced:
  - [SPI_fnumber](../S/SPI_fnumber.md) (gets attribute number by name)
  - [SPI_getbinval](../S/SPI_getbinval.md) (extracts binary attribute value)
  - [SPI_getvalue](../S/SPI_getvalue.md) (extracts string attribute value)
  - [DatumGetName](../D/DatumGetName.md), DatumGetChar, DatumGetObjectId, DatumGetBool (datum conversion functions)
  - [stringToNode](../s/stringToNode.md) (parses stored node trees)
  - [quote_identifier](../q/quote_identifier.md) (quotes SQL identifiers)
  - [generate_relation_name](../g/generate_relation_name.md), generate_qualified_relation_name (formats relation names)
  - [getInsertSelectQuery](../g/getInsertSelectQuery.md) (handles INSERT...SELECT rules)
  - [AcquireRewriteLocks](../A/AcquireRewriteLocks.md) (acquires necessary locks)
  - [set_deparse_for_query](../s/set_deparse_for_query.md) (sets up deparse context)
  - [get_rule_expr](../g/get_rule_expr.md) (deparses qualification expressions)
  - [get_query_def](../g/get_query_def.md) (deparses action queries)
- Called from (representative examples):
  - [pg_get_ruledef_worker](../p/pg_get_ruledef_worker.md)

## Notes and Other Information
- This is a static function within ruleutils.c for internal use within the rule deparsing subsystem
- Handles all four PostgreSQL rule event types: SELECT (views), UPDATE, INSERT, and DELETE
- Properly manages deparse context for handling OLD and NEW variable references in rule qualifications
- Uses table locking (AccessShareLock) when accessing the target relation
- Supports both simple single-action rules and complex multi-action rules with parentheses
- Implements proper pretty-printing with configurable indentation and schema qualification
- Critical component of PostgreSQL's rule system introspection functionality
- Part of the pg_get_ruledef() SQL function implementation

## Simplified Source

```c
static void make_ruledef(StringInfo buf, HeapTuple ruletup, TupleDesc rulettc,
                        int prettyFlags) {
    char *rulename;
    char ev_type;
    Oid ev_class;
    bool is_instead;
    char *ev_qual;
    char *ev_action;
    List *actions;
    Relation ev_relation;
    TupleDesc viewResultDesc = NULL;
    int fno;
    Datum dat;
    bool isnull;

    // Extract rule attributes from tuple
    fno = SPI_fnumber(rulettc, "rulename");
    dat = SPI_getbinval(ruletup, rulettc, fno, &isnull);
    rulename = NameStr(*(DatumGetName(dat)));

    fno = SPI_fnumber(rulettc, "ev_type");
    dat = SPI_getbinval(ruletup, rulettc, fno, &isnull);
    ev_type = DatumGetChar(dat);

    fno = SPI_fnumber(rulettc, "ev_class");
    dat = SPI_getbinval(ruletup, rulettc, fno, &isnull);
    ev_class = DatumGetObjectId(dat);

    fno = SPI_fnumber(rulettc, "is_instead");
    dat = SPI_getbinval(ruletup, rulettc, fno, &isnull);
    is_instead = DatumGetBool(dat);

    fno = SPI_fnumber(rulettc, "ev_qual");
    ev_qual = SPI_getvalue(ruletup, rulettc, fno);

    fno = SPI_fnumber(rulettc, "ev_action");
    ev_action = SPI_getvalue(ruletup, rulettc, fno);
    actions = (List *) stringToNode(ev_action);

    ev_relation = table_open(ev_class, AccessShareLock);

    // Build the CREATE RULE command
    appendStringInfo(buf, "CREATE RULE %s AS", quote_identifier(rulename));

    if (prettyFlags & PRETTYFLAG_INDENT)
        appendStringInfoString(buf, "\n    ON ");
    else
        appendStringInfoString(buf, " ON ");

    // Add event type
    switch (ev_type) {
        case '1':
            appendStringInfoString(buf, "SELECT");
            viewResultDesc = RelationGetDescr(ev_relation);
            break;
        case '2':
            appendStringInfoString(buf, "UPDATE");
            break;
        case '3':
            appendStringInfoString(buf, "INSERT");
            break;
        case '4':
            appendStringInfoString(buf, "DELETE");
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("rule \"%s\" has unsupported event type %d",
                                  rulename, ev_type)));
    }

    // Add target relation
    appendStringInfo(buf, " TO %s",
                    (prettyFlags & PRETTYFLAG_SCHEMA) ?
                    generate_relation_name(ev_class, NIL) :
                    generate_qualified_relation_name(ev_class));

    // Add WHERE clause if present
    if (strcmp(ev_qual, "<>") != 0) {
        Node *qual;
        Query *query;
        deparse_context context;
        deparse_namespace dpns;

        if (prettyFlags & PRETTYFLAG_INDENT)
            appendStringInfoString(buf, "\n  ");
        appendStringInfoString(buf, " WHERE ");

        qual = stringToNode(ev_qual);
        query = (Query *) linitial(actions);
        query = getInsertSelectQuery(query, NULL);
        AcquireRewriteLocks(query, false, false);

        // Set up deparse context
        context.buf = buf;
        context.namespaces = list_make1(&dpns);
        context.resultDesc = NULL;
        context.targetList = NIL;
        context.windowClause = NIL;
        context.varprefix = (list_length(query->rtable) != 1);
        context.prettyFlags = prettyFlags;
        context.wrapColumn = WRAP_COLUMN_DEFAULT;
        context.indentLevel = PRETTYINDENT_STD;
        context.colNamesVisible = true;
        context.inGroupBy = false;
        context.varInOrderBy = false;
        context.appendparents = NULL;

        set_deparse_for_query(&dpns, query, NIL);
        get_rule_expr(qual, &context, false);
    }

    appendStringInfoString(buf, " DO ");

    // Add INSTEAD keyword if applicable
    if (is_instead)
        appendStringInfoString(buf, "INSTEAD ");

    // Add rule actions
    if (list_length(actions) > 1) {
        // Multiple actions - wrap in parentheses
        ListCell *action;
        Query *query;

        appendStringInfoChar(buf, '(');
        foreach(action, actions) {
            query = (Query *) lfirst(action);
            get_query_def(query, buf, NIL, viewResultDesc, true,
                         prettyFlags, WRAP_COLUMN_DEFAULT, 0);
            if (prettyFlags)
                appendStringInfoString(buf, ";\n");
            else
                appendStringInfoString(buf, "; ");
        }
        appendStringInfoString(buf, ");");
    } else {
        // Single action
        Query *query = (Query *) linitial(actions);
        get_query_def(query, buf, NIL, viewResultDesc, true,
                     prettyFlags, WRAP_COLUMN_DEFAULT, 0);
        appendStringInfoChar(buf, ';');
    }

    table_close(ev_relation, AccessShareLock);
}
```