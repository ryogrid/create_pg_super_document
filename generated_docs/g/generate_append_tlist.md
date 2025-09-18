# generate_append_tlist

## Location
src/backend/optimizer/prep/prepunion.c: 1546 - 1673

## Overview
Generates a targetlist for a set-operation Append node, creating simple Var nodes with appropriate datatypes, typmods, and collations for combining multiple input relations.

## Definition


## Detailed Description
This function constructs a targetlist for Append plan nodes used in set operations by creating simple Var expressions that reference columns from the input subplans. Unlike generate_setop_tlist, this function creates Vars with varno 0 and focuses on determining the appropriate typmod for each column by examining all input targetlists. If all input subplans agree on both the datatype and typmod for a column, that typmod is used; otherwise, typmod is set to -1 to indicate unknown/variable precision.

The function first analyzes all input targetlists to determine the most appropriate typmod for each output column, then constructs the output targetlist with the determined datatypes, typmods, and collations. All entries are simple Vars that will be resolved during execution to reference the appropriate input subplan columns.

## Parameters / Member Variables
- : OID list of the set-operation's result column datatypes
- : OID list of the set-operation's result column collations
- : true to create a resjunk flag column copied up from subplans
- : list of targetlists for sub-plans of the Append node
- : targetlist to take column names from

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - list_length
  - list_head
  - exprType
  - exprTypmod
  - [lnext](../l/lnext.md)
  - makeVar
  - [makeTargetEntry](../m/makeTargetEntry.md)
  - lappend
  - [pfree](../p/pfree.md)
  - forthree (macro for iterating over three lists)
- Called from:
  - [generate_recursion_path](generate_recursion_path.md)
  - [generate_union_paths](generate_union_paths.md)  
  - [generate_nonunion_paths](generate_nonunion_paths.md)

## Notes and Other Information
- All generated Vars use varno 0, which indicates they reference the current plan node's output
- Typmod determination is conservative: disagreement among inputs forces typmod to -1
- The function follows the same convention as generate_setop_tlist by setting ressortgroupref equal to resno for all non-resjunk columns
- The flag column, when requested, is created as a resjunk Var that references a flag column from the input subplans
- A known limitation is that set_pathtarget_cost_width cannot determine realistic width estimates for the varno-zero targetlist produced by this function