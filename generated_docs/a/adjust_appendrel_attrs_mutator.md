# adjust_appendrel_attrs_mutator

## Location
[src/backend/optimizer/util/appendinfo.c:215-520](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/appendinfo.c#L215-L520)

## Overview
The core recursive function that performs the actual transformation of expression trees, translating parent relation references to child relation references using AppendRelInfo mappings.

## Definition


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