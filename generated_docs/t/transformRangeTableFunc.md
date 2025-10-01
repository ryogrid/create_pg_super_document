# transformRangeTableFunc

## Location
[src/backend/parser/parse_clause.c:688-909](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L688-L909)

## Overview
Transforms a raw RangeTableFunc (currently XMLTABLE construct) into a TableFunc structure, processing namespace clauses, document expression, row expression, column specifications, and default values.

## Definition
static ParseNamespaceItem *
transformRangeTableFunc(ParseState *pstate, RangeTableFunc *rtf)

## Detailed Description
The transformRangeTableFunc function handles the transformation of table function constructs, currently specifically supporting XMLTABLE functionality. The function creates a TableFunc structure and processes all components: it transforms and type-coerces the row-generating and document-generating expressions to appropriate types (TEXT and XML respectively), processes column specifications including FOR ORDINALITY columns, handles namespace declarations with validation for uniqueness and single default namespace, and manages type information including collations. The function also enables lateral references and determines whether the RTE should be marked as LATERAL based on cross-references or explicit specification.

## Parameters / Member Variables
- pstate: ParseState structure containing the current parsing context and state information  
- rtf: RangeTableFunc structure representing the raw table function construct to be transformed, including expressions, column definitions, namespaces, and lateral flag

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [transformExpr](transformExpr.md)
  - [coerce_to_specific_type](../c/coerce_to_specific_type.md)
  - [coerce_to_specific_type_typmod](../c/coerce_to_specific_type_typmod.md)
  - [assign_expr_collations](../a/assign_expr_collations.md)
  - [typenameTypeIdAndMod](typenameTypeIdAndMod.md)
  - [get_typcollation](../g/get_typcollation.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [contain_vars_of_level](../c/contain_vars_of_level.md)
  - [addRangeTableEntryForTableFunc](../a/addRangeTableEntryForTableFunc.md)
  - TFT_XMLTABLE
  - EXPR_KIND_FROM_FUNCTION
- Called from (representative examples):
  - [transformFromClauseItem](transformFromClauseItem.md)

## Notes and Other Information
- Currently only supports XMLTABLE functionality (TFT_XMLTABLE), with JSON_TABLE support handled elsewhere
- Automatically enables lateral references (p_lateral_active = true) for SQL spec compliance
- Row expression is coerced to TEXT type, document expression to XML type
- FOR ORDINALITY columns are automatically assigned INT4OID type with typmod -1
- Only one FOR ORDINALITY column is allowed per table function
- Column names must be unique within the table function
- Namespace processing validates uniqueness of named namespaces and allows only one default namespace
- Default namespace is represented internally as NULL pointer
- SETOF types are not allowed for individual columns
- Column expressions (PATH) are coerced to TEXT, default expressions to the target column type
- The function maintains NOT NULL information using a bitmap (tf->notnulls)
- Type collations are automatically assigned for all expressions and columns

## Simplified Source

```c
static ParseNamespaceItem *transformRangeTableFunc(ParseState *pstate, RangeTableFunc *rtf)
{
    TableFunc *tf = makeNode(TableFunc);
    const char *constructName;
    Oid docType;
    bool is_lateral;
    char **names;
    int colno;

    // Currently only XMLTABLE is supported
    tf->functype = TFT_XMLTABLE;
    constructName = "XMLTABLE";
    docType = XMLOID;

    // Enable lateral references for SQL spec compliance
    pstate->p_lateral_active = true;

    // Transform and type-coerce row and document expressions
    tf->rowexpr = coerce_to_specific_type(pstate,
                                         transformExpr(pstate, rtf->rowexpr, EXPR_KIND_FROM_FUNCTION),
                                         TEXTOID, constructName);
    assign_expr_collations(pstate, tf->rowexpr);

    tf->docexpr = coerce_to_specific_type(pstate,
                                         transformExpr(pstate, rtf->docexpr, EXPR_KIND_FROM_FUNCTION),
                                         docType, constructName);
    assign_expr_collations(pstate, tf->docexpr);

    tf->ordinalitycol = -1;  // No ordinality column initially

    // Process column specifications
    names = palloc(sizeof(char *) * list_length(rtf->columns));
    colno = 0;
    foreach(col, rtf->columns)
    {
        RangeTableFuncCol *rawc = (RangeTableFuncCol *) lfirst(col);
        Oid typid;
        int32 typmod;
        Node *colexpr;
        Node *coldefexpr;

        tf->colnames = lappend(tf->colnames, makeString(pstrdup(rawc->colname)));

        // Handle FOR ORDINALITY columns
        if (rawc->for_ordinality)
        {
            if (tf->ordinalitycol != -1)
                ereport(ERROR, "only one FOR ORDINALITY column is allowed");
            typid = INT4OID;
            typmod = -1;
            tf->ordinalitycol = colno;
        }
        else
        {
            if (rawc->typeName->setof)
                ereport(ERROR, "column cannot be declared SETOF");
            typenameTypeIdAndMod(pstate, rawc->typeName, &typid, &typmod);
        }

        tf->coltypes = lappend_oid(tf->coltypes, typid);
        tf->coltypmods = lappend_int(tf->coltypmods, typmod);
        tf->colcollations = lappend_oid(tf->colcollations, get_typcollation(typid));

        // Transform PATH and DEFAULT expressions
        if (rawc->colexpr)
        {
            colexpr = coerce_to_specific_type(pstate,
                                             transformExpr(pstate, rawc->colexpr, EXPR_KIND_FROM_FUNCTION),
                                             TEXTOID, constructName);
            assign_expr_collations(pstate, colexpr);
        }
        else
            colexpr = NULL;

        if (rawc->coldefexpr)
        {
            coldefexpr = coerce_to_specific_type_typmod(pstate,
                                                       transformExpr(pstate, rawc->coldefexpr, EXPR_KIND_FROM_FUNCTION),
                                                       typid, typmod, constructName);
            assign_expr_collations(pstate, coldefexpr);
        }
        else
            coldefexpr = NULL;

        tf->colexprs = lappend(tf->colexprs, colexpr);
        tf->coldefexprs = lappend(tf->coldefexprs, coldefexpr);

        if (rawc->is_not_null)
            tf->notnulls = bms_add_member(tf->notnulls, colno);

        // Check for unique column names
        for (j = 0; j < colno; j++)
            if (strcmp(names[j], rawc->colname) == 0)
                ereport(ERROR, "column name is not unique");
        names[colno] = rawc->colname;

        colno++;
    }
    pfree(names);

    // Process namespace declarations
    if (rtf->namespaces != NIL)
    {
        List *ns_uris = NIL;
        List *ns_names = NIL;
        bool default_ns_seen = false;

        foreach(ns, rtf->namespaces)
        {
            ResTarget *r = (ResTarget *) lfirst(ns);
            Node *ns_uri;

            ns_uri = transformExpr(pstate, r->val, EXPR_KIND_FROM_FUNCTION);
            ns_uri = coerce_to_specific_type(pstate, ns_uri, TEXTOID, constructName);
            assign_expr_collations(pstate, ns_uri);
            ns_uris = lappend(ns_uris, ns_uri);

            // Validate namespace uniqueness
            if (r->name != NULL)
            {
                // Check for duplicate namespace names
                foreach(lc2, ns_names)
                {
                    String *ns_node = lfirst_node(String, lc2);
                    if (ns_node && strcmp(strVal(ns_node), r->name) == 0)
                        ereport(ERROR, "namespace name is not unique");
                }
            }
            else
            {
                if (default_ns_seen)
                    ereport(ERROR, "only one default namespace is allowed");
                default_ns_seen = true;
            }

            ns_names = lappend(ns_names, r->name ? makeString(r->name) : NULL);
        }

        tf->ns_uris = ns_uris;
        tf->ns_names = ns_names;
    }

    tf->location = rtf->location;
    pstate->p_lateral_active = false;

    // Determine if LATERAL marking is needed
    is_lateral = rtf->lateral || contain_vars_of_level((Node *) tf, 0);

    return addRangeTableEntryForTableFunc(pstate, tf, rtf->alias, is_lateral, true);
}
```