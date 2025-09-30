# adjust_appendrel_attrs_mutator

## Location
[src/backend/optimizer/util/appendinfo.c:215-520](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/appendinfo.c#L215-L520)

## Overview
The core recursive function that performs the actual transformation of expression trees, translating parent relation references to child relation references using AppendRelInfo mappings.

## Definition

```c
structures, either. */
	Assert(!IsA(node, RangeTblRef));
```
## Detailed Description
This function implements a comprehensive expression tree walker that handles the complex task of translating variable references and relation identifiers from parent tables to child tables. It processes various node types including Var nodes, whole-row references, PlaceHolderVars, RestrictInfo nodes, and CurrentOfExpr nodes. The function handles special cases like ROWID_VAR placeholders, maintains nulling relations for outer joins, and performs proper type coercions when translating whole-row variables between relations with different tuple layouts.

## Parameters / Member Variables
- : The expression tree node to be transformed
- : Context structure containing AppendRelInfo mappings and PlannerInfo

## Dependencies
- Functions called/Symbols referenced:
  - copyObject (creates deep copies of nodes)
  - [list_nth](../l/list_nth.md) (accesses list elements)
  - [get_rel_name](../g/get_rel_name.md) (retrieves relation names for error messages)
  - [makeNullConst](../m/makeNullConst.md) (creates NULL constants)
  - expression_tree_mutator (recursively processes expression trees)
  - [adjust_child_relids](adjust_child_relids.md) (adjusts relation ID sets)
  - rt_fetch (retrieves range table entries)
  - [bms_is_member](../b/bms_is_member.md) (tests bitmap membership)
- Called from (representative examples):
  - [adjust_appendrel_attrs](adjust_appendrel_attrs.md)
  - [adjust_appendrel_attrs_mutator](adjust_appendrel_attrs_mutator.md) (recursive calls)

## Notes and Other Information
- Handles Var nodes by looking up translations in AppendRelInfo->translated_vars
- Special processing for whole-row Vars (varattno == 0) with tuple layout conversion
- ROWID_VAR placeholders are resolved to specific leaf relation variables when possible
- [RestrictInfo](../R/RestrictInfo.md) nodes require special handling to preserve optimizer metadata
- Includes extensive assertions to prevent processing of inappropriate node types
- Maintains varnullingrels information for outer join semantics
- Returns NULL constants when child relations cannot provide requested row identity values

## Simplified Source

```c
static Node *
adjust_appendrel_attrs_mutator(Node *node,
                              adjust_appendrel_attrs_context *context)
{
    if (node == NULL)
        return NULL;

    // Handle Var nodes - main case for column references
    if (IsA(node, Var))
    {
        Var *var = (Var *) copyObject(node);
        AppendRelInfo *appinfo = NULL;

        // Skip if not at current query level
        if (var->varlevelsup != 0)
            return (Node *) var;

        // Find matching AppendRelInfo for this relation
        for (int cnt = 0; cnt < context->nappinfos; cnt++)
        {
            if (var->varno == context->appinfos[cnt]->parent_relid)
            {
                appinfo = context->appinfos[cnt];
                break;
            }
        }

        if (appinfo)
        {
            // Update relation ID to child
            var->varno = appinfo->child_relid;
            var->varnosyn = 0;
            var->varattnosyn = 0;

            if (var->varattno > 0)
            {
                // Translate attribute using translated_vars mapping
                Node *newnode = copyObject(list_nth(appinfo->translated_vars,
                                                   var->varattno - 1));

                // Preserve nulling relations for outer joins
                if (IsA(newnode, Var))
                    ((Var *) newnode)->varnullingrels = var->varnullingrels;

                return newnode;
            }
            else if (var->varattno == 0)
            {
                // Handle whole-row references with type conversion if needed
                if (OidIsValid(appinfo->child_reltype) &&
                    appinfo->parent_reltype != appinfo->child_reltype)
                {
                    // Create row type conversion expression
                    ConvertRowtypeExpr *r = makeNode(ConvertRowtypeExpr);
                    r->arg = (Expr *) var;
                    r->resulttype = appinfo->parent_reltype;
                    r->convertformat = COERCE_IMPLICIT_CAST;
                    var->vartype = appinfo->child_reltype;
                    return (Node *) r;
                }
            }
        }

        return (Node *) var;
    }

    // Handle CurrentOfExpr nodes
    if (IsA(node, CurrentOfExpr))
    {
        CurrentOfExpr *cexpr = (CurrentOfExpr *) copyObject(node);

        // Update cursor variable relation ID
        for (int cnt = 0; cnt < context->nappinfos; cnt++)
        {
            if (cexpr->cvarno == context->appinfos[cnt]->parent_relid)
            {
                cexpr->cvarno = context->appinfos[cnt]->child_relid;
                break;
            }
        }
        return (Node *) cexpr;
    }

    // Handle PlaceHolderVar nodes
    if (IsA(node, PlaceHolderVar))
    {
        PlaceHolderVar *phv = (PlaceHolderVar *)
            expression_tree_mutator(node, adjust_appendrel_attrs_mutator,
                                   (void *) context);

        // Adjust relation ID sets
        if (phv->phlevelsup == 0)
            phv->phrels = adjust_child_relids(phv->phrels,
                                            context->nappinfos,
                                            context->appinfos);
        return (Node *) phv;
    }

    // Handle RestrictInfo nodes with special optimizer metadata
    if (IsA(node, RestrictInfo))
    {
        RestrictInfo *oldinfo = (RestrictInfo *) node;
        RestrictInfo *newinfo = makeNode(RestrictInfo);

        // Copy all fields and recursively process clauses
        memcpy(newinfo, oldinfo, sizeof(RestrictInfo));
        newinfo->clause = (Expr *)
            adjust_appendrel_attrs_mutator((Node *) oldinfo->clause, context);
        newinfo->orclause = (Expr *)
            adjust_appendrel_attrs_mutator((Node *) oldinfo->orclause, context);

        // Adjust all relation ID sets
        newinfo->clause_relids = adjust_child_relids(oldinfo->clause_relids,
                                                    context->nappinfos,
                                                    context->appinfos);
        // Reset cached values that need recalculation
        newinfo->eval_cost.startup = -1;
        newinfo->norm_selec = -1;

        return (Node *) newinfo;
    }

    // Recursively process all other expression nodes
    return expression_tree_mutator(node, adjust_appendrel_attrs_mutator,
                                  (void *) context);
}
```