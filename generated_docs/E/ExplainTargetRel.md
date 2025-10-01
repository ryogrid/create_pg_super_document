# ExplainTargetRel

## Location
[src/backend/commands/explain.c:4034-4171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L4034-L4171)

## Overview
ExplainTargetRel is a static function that displays the target relation information for various types of scan and modify operations in PostgreSQL's EXPLAIN output.

## Definition
```c
static void ExplainTargetRel(Plan *plan, Index rti, ExplainState *es)
```

## Detailed Description
This function is responsible for showing the target relation of scan or modify nodes in EXPLAIN output. It handles a wide variety of plan node types including sequential scans, index scans, function scans, table function scans, CTE scans, and modify operations. The function extracts the appropriate object name (relation name, function name, CTE name, etc.) based on the plan node type and formats it according to the EXPLAIN output format (text or structured). It also handles namespace information and alias names when verbose mode is enabled.

## Parameters / Member Variables
- `plan`: Pointer to the Plan node for which to show the target relation
- `rti`: Range Table Index identifying which range table entry to use  
- `es`: Pointer to the ExplainState structure controlling output format and options

## Dependencies
- Functions called/Symbols referenced:
  - rt_fetch
  - [list_nth](../l/list_nth.md)  
  - nodeTag
  - [get_rel_name](../g/get_rel_name.md)
  - [get_namespace_name_or_temp](../g/get_namespace_name_or_temp.md)
  - [get_rel_namespace](../g/get_rel_namespace.md)
  - [get_func_name](../g/get_func_name.md)
  - [get_func_namespace](../g/get_func_namespace.md)
  - [quote_identifier](../q/quote_identifier.md)
  - [ExplainPropertyText](ExplainPropertyText.md)
- Called from (representative examples):
  - [ExplainScanTarget](ExplainScanTarget.md)
  - [ExplainModifyTarget](ExplainModifyTarget.md)
  - [show_modifytable_info](../s/show_modifytable_info.md)

## Notes and Other Information
- This is a static function, only accessible within the explain.c file
- Handles numerous plan node types: SeqScan, IndexScan, FunctionScan, TableFuncScan, ValuesScan, CteScan, ModifyTable, etc.
- Supports both text and structured (JSON/XML/YAML) output formats
- In verbose mode, includes schema/namespace information
- For function scans, attempts to extract the actual function name when possible
- Handles special cases like CTE self-references (WorkTableScan vs CteScan)
- Part of PostgreSQL's comprehensive query execution plan explanation system

## Simplified Source

```c
static void
ExplainTargetRel(Plan *plan, Index rti, ExplainState *es)
{
    char *objectname = NULL;
    char *namespace = NULL;
    const char *objecttag = NULL;
    RangeTblEntry *rte;
    char *refname;

    // Get the range table entry and reference name
    rte = rt_fetch(rti, es->rtable);
    refname = (char *) list_nth(es->rtable_names, rti - 1);
    if (refname == NULL)
        refname = rte->eref->aliasname;

    // Extract object information based on plan node type
    switch (nodeTag(plan))
    {
        case T_SeqScan:
        case T_IndexScan:
        case T_BitmapHeapScan:
        case T_ModifyTable:
            // Regular table scans
            objectname = get_rel_name(rte->relid);
            if (es->verbose)
                namespace = get_namespace_name_or_temp(get_rel_namespace(rte->relid));
            objecttag = "Relation Name";
            break;

        case T_FunctionScan:
            // Function scans - try to get function name
            if (list_length(((FunctionScan *) plan)->functions) == 1) {
                RangeTblFunction *rtfunc = linitial(((FunctionScan *) plan)->functions);
                if (IsA(rtfunc->funcexpr, FuncExpr)) {
                    FuncExpr *funcexpr = (FuncExpr *) rtfunc->funcexpr;
                    objectname = get_func_name(funcexpr->funcid);
                    if (es->verbose)
                        namespace = get_namespace_name_or_temp(get_func_namespace(funcexpr->funcid));
                }
            }
            objecttag = "Function Name";
            break;

        case T_CteScan:
        case T_WorkTableScan:
            // CTE scans
            objectname = rte->ctename;
            objecttag = "CTE Name";
            break;

        case T_NamedTuplestoreScan:
            // Named tuplestore scans
            objectname = rte->enrname;
            objecttag = "Tuplestore Name";
            break;
    }

    // Format output based on explain format
    if (es->format == EXPLAIN_FORMAT_TEXT) {
        appendStringInfoString(es->str, " on");
        if (namespace != NULL)
            appendStringInfo(es->str, " %s.%s", quote_identifier(namespace), quote_identifier(objectname));
        else if (objectname != NULL)
            appendStringInfo(es->str, " %s", quote_identifier(objectname));
        if (objectname == NULL || strcmp(refname, objectname) != 0)
            appendStringInfo(es->str, " %s", quote_identifier(refname));
    } else {
        // Structured format output
        if (objecttag != NULL && objectname != NULL)
            ExplainPropertyText(objecttag, objectname, es);
        if (namespace != NULL)
            ExplainPropertyText("Schema", namespace, es);
        ExplainPropertyText("Alias", refname, es);
    }
}
```