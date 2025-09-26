# makeVarFromTargetEntry

## Location
[src/backend/nodes/makefuncs.c:105-134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/makefuncs.c#L105-L134)

## Overview
Creates a Var node from a TargetEntry, extracting type information from the target's expression to build a same-level variable reference.

## Definition
```c
Var *makeVarFromTargetEntry(int varno, TargetEntry *tle)
```

## Detailed Description
The makeVarFromTargetEntry function is a convenience utility that constructs a Var node based on information from a TargetEntry. It extracts the type, type modifier, and collation information from the TargetEntry's expression using PostgreSQL's expression analysis functions, then calls makeVar with a varlevelsup of 0 (indicating a same-level reference). This function is commonly used when transforming query target lists or when creating variable references that correspond to specific target list entries.

## Parameters
- `varno`: Integer index of the variable's relation in the range table where this Var will be used
- `tle`: Pointer to the TargetEntry from which to extract type and attribute information

## Dependencies
- Functions called/Symbols referenced:
  - makeVar (creates the actual Var node)
  - exprType (extracts type OID from expression)
  - exprTypmod (extracts type modifier from expression)
  - exprCollation (extracts collation OID from expression)
  - TargetEntry (struct type from primnodes.h)
- Called from (representative examples):
  - coerce_fn_result_column
  - search_indexed_tlist_for_phv
  - search_indexed_tlist_for_non_var
  - search_indexed_tlist_for_sortgroupref
  - generate_subquery_vars
  - make_setop_translation_list
  - build_physical_tlist
  - transformInsertStmt

## Notes and Other Information
- Uses TargetEntry's resno as the varattno for the created Var
- Always creates same-level variables (varlevelsup = 0) - not suitable for outer references
- Automatically derives type information from the TargetEntry's expression rather than requiring explicit type parameters
- Particularly useful in subquery processing and target list transformations
- Part of the makefuncs.c utility collection for node construction