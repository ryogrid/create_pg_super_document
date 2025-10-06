# pg_get_triggerdef_worker

## Location
[src/backend/utils/adt/ruleutils.c:880-1157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L880-L1157)

## Overview
A static worker function that constructs the complete CREATE TRIGGER statement for a given trigger OID by querying the system catalog and building the DDL string.

## Definition

```c
static char *
pg_get_triggerdef_worker(Oid trigid, bool pretty)
```
## Detailed Description
This function performs the core work of reconstructing a trigger definition from PostgreSQL's system catalogs. It queries the pg_trigger system table to retrieve trigger metadata, then constructs a complete CREATE TRIGGER statement including timing (BEFORE/AFTER/INSTEAD OF), events (INSERT/UPDATE/DELETE/TRUNCATE), target table, constraint information, transition table references, row/statement level specification, WHEN clause if present, and the trigger function with arguments. The function handles both pretty-printed output (with selective schema qualification) and non-pretty output (with full schema qualification for safety).

## Parameters / Member Variables
- `trigid`: OID of the trigger to retrieve the definition for
- `pretty`: Boolean flag controlling output formatting - when true, uses readable formatting and selective schema qualification; when false, always uses full schema qualification
## Dependencies
- Functions called/Symbols referenced:
  - : Opens the pg_trigger system catalog
  - : Initializes scan key for trigger lookup
  - : Begins system catalog scan
  - : Gets next tuple from scan
  - : Validates retrieved tuple
  - : Casts tuple to trigger struct
  - : Initializes string buffer
  - : Appends formatted text to buffer
  - : Quotes SQL identifiers for safety
  -  macros: Tests trigger timing and events
  - : Retrieves column names for UPDATE OF triggers
  - : Gets relation name with optional schema qualification
  - : Extracts attributes from heap tuple
  - : Parses stored expression text
  - : Deparses expressions for WHEN clause
  - : Gets trigger function name
  - : Quotes string literals
- Called from (representative examples):
  - : Standard trigger definition function
  - : Extended trigger definition function

## Notes and Other Information
- Handles all trigger types: row-level, statement-level, constraint triggers
- Supports WHEN clause reconstruction with proper variable context (OLD/NEW)
- Manages transition table references (REFERENCING OLD TABLE AS/NEW TABLE AS)
- Processes trigger arguments embedded in tgargs bytea field
- Uses deparse context to properly format complex WHEN expressions
- Returns NULL if trigger OID is not found in system catalog
- Part of PostgreSQL's rule utilities for DDL reconstruction
- Located in src/backend/utils/adt/ruleutils.c:880-1157
- Critical component for pg_dump, system introspection, and trigger administration

## Simplified Source

```c
static char *pg_get_triggerdef_worker(Oid trigid, bool pretty) {
    HeapTuple ht_trig;
    Form_pg_trigger trigrec;
    StringInfoData buf;

    // Open pg_trigger catalog and scan for the trigger
    Relation tgrel = table_open(TriggerRelationId, AccessShareLock);
    ScanKeyData skey[1];
    ScanKeyInit(&skey[0], Anum_pg_trigger_oid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(trigid));

    SysScanDesc tgscan = systable_beginscan(tgrel, TriggerOidIndexId, true, NULL, 1, skey);
    ht_trig = systable_getnext(tgscan);

    // Return NULL if trigger not found
    if (!HeapTupleIsValid(ht_trig)) {
        systable_endscan(tgscan);
        table_close(tgrel, AccessShareLock);
        return NULL;
    }

    trigrec = (Form_pg_trigger) GETSTRUCT(ht_trig);
    initStringInfo(&buf);

    // Build CREATE [CONSTRAINT] TRIGGER name
    char *tgname = NameStr(trigrec->tgname);
    appendStringInfo(&buf, "CREATE %sTRIGGER %s ",
                     OidIsValid(trigrec->tgconstraint) ? "CONSTRAINT " : "",
                     quote_identifier(tgname));

    // Add timing: BEFORE/AFTER/INSTEAD OF
    if (TRIGGER_FOR_BEFORE(trigrec->tgtype))
        appendStringInfoString(&buf, "BEFORE");
    else if (TRIGGER_FOR_AFTER(trigrec->tgtype))
        appendStringInfoString(&buf, "AFTER");
    else if (TRIGGER_FOR_INSTEAD(trigrec->tgtype))
        appendStringInfoString(&buf, "INSTEAD OF");

    // Add events: INSERT/UPDATE/DELETE/TRUNCATE with OR connectors
    int findx = 0;
    if (TRIGGER_FOR_INSERT(trigrec->tgtype)) {
        appendStringInfoString(&buf, " INSERT");
        findx++;
    }
    if (TRIGGER_FOR_DELETE(trigrec->tgtype)) {
        appendStringInfoString(&buf, findx > 0 ? " OR DELETE" : " DELETE");
        findx++;
    }
    if (TRIGGER_FOR_UPDATE(trigrec->tgtype)) {
        appendStringInfoString(&buf, findx > 0 ? " OR UPDATE" : " UPDATE");
        // Handle UPDATE OF column_list if specified
        if (trigrec->tgattr.dim1 > 0) {
            appendStringInfoString(&buf, " OF ");
            for (int i = 0; i < trigrec->tgattr.dim1; i++) {
                if (i > 0) appendStringInfoString(&buf, ", ");
                char *attname = get_attname(trigrec->tgrelid, trigrec->tgattr.values[i], false);
                appendStringInfoString(&buf, quote_identifier(attname));
            }
        }
        findx++;
    }
    if (TRIGGER_FOR_TRUNCATE(trigrec->tgtype)) {
        appendStringInfoString(&buf, findx > 0 ? " OR TRUNCATE" : " TRUNCATE");
    }

    // Add target table with optional schema qualification
    appendStringInfo(&buf, " ON %s ",
                     pretty ? generate_relation_name(trigrec->tgrelid, NIL) :
                             generate_qualified_relation_name(trigrec->tgrelid));

    // Handle constraint trigger options (FROM, DEFERRABLE, INITIALLY)
    if (OidIsValid(trigrec->tgconstraint)) {
        if (OidIsValid(trigrec->tgconstrrelid))
            appendStringInfo(&buf, "FROM %s ", generate_relation_name(trigrec->tgconstrrelid, NIL));
        if (!trigrec->tgdeferrable)
            appendStringInfoString(&buf, "NOT ");
        appendStringInfoString(&buf, "DEFERRABLE INITIALLY ");
        appendStringInfoString(&buf, trigrec->tginitdeferred ? "DEFERRED " : "IMMEDIATE ");
    }

    // Handle transition table references (REFERENCING OLD/NEW TABLE AS)
    Datum value;
    bool isnull;
    char *tgoldtable = NULL, *tgnewtable = NULL;

    value = fastgetattr(ht_trig, Anum_pg_trigger_tgoldtable, tgrel->rd_att, &isnull);
    if (!isnull) tgoldtable = NameStr(*DatumGetName(value));

    value = fastgetattr(ht_trig, Anum_pg_trigger_tgnewtable, tgrel->rd_att, &isnull);
    if (!isnull) tgnewtable = NameStr(*DatumGetName(value));

    if (tgoldtable != NULL || tgnewtable != NULL) {
        appendStringInfoString(&buf, "REFERENCING ");
        if (tgoldtable != NULL)
            appendStringInfo(&buf, "OLD TABLE AS %s ", quote_identifier(tgoldtable));
        if (tgnewtable != NULL)
            appendStringInfo(&buf, "NEW TABLE AS %s ", quote_identifier(tgnewtable));
    }

    // Add FOR EACH ROW/STATEMENT
    appendStringInfoString(&buf, TRIGGER_FOR_ROW(trigrec->tgtype) ?
                          "FOR EACH ROW " : "FOR EACH STATEMENT ");

    // Handle WHEN clause if present
    value = fastgetattr(ht_trig, Anum_pg_trigger_tgqual, tgrel->rd_att, &isnull);
    if (!isnull) {
        appendStringInfoString(&buf, "WHEN (");
        // Build deparse context for OLD/NEW references and format the expression
        Node *qual = stringToNode(TextDatumGetCString(value));
        deparse_context context;
        deparse_namespace dpns;
        // ... setup deparse context with OLD/NEW RTEs ...
        get_rule_expr(qual, &context, false);
        appendStringInfoString(&buf, ") ");
    }

    // Add EXECUTE FUNCTION with arguments
    appendStringInfo(&buf, "EXECUTE FUNCTION %s(",
                     generate_function_name(trigrec->tgfoid, 0, NIL, NULL, false, NULL, false));

    // Add trigger arguments if any
    if (trigrec->tgnargs > 0) {
        value = fastgetattr(ht_trig, Anum_pg_trigger_tgargs, tgrel->rd_att, &isnull);
        char *p = (char *) VARDATA_ANY(DatumGetByteaPP(value));
        for (int i = 0; i < trigrec->tgnargs; i++) {
            if (i > 0) appendStringInfoString(&buf, ", ");
            simple_quote_literal(&buf, p);
            while (*p) p++; // advance to next string
            p++;
        }
    }

    appendStringInfoChar(&buf, ')');

    // Cleanup and return
    systable_endscan(tgscan);
    table_close(tgrel, AccessShareLock);
    return buf.data;
}
```