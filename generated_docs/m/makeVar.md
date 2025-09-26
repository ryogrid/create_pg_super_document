# makeVar

## Location
[src/backend/nodes/makefuncs.c:66-104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/makefuncs.c#L66-L104)

## Overview
Creates and initializes a Var node, which represents variable references to table columns or expressions in PostgreSQL's expression tree.

## Definition
```c
Var *makeVar(int varno, AttrNumber varattno, Oid vartype, int32 vartypmod, Oid varcollid, Index varlevelsup)
```

## Detailed Description
The makeVar function is a fundamental constructor utility that creates Var nodes, which are essential components of PostgreSQL's expression system. Var nodes represent references to columns in tables or other relations within the query execution context. The function initializes all the core fields needed to identify and access the referenced column, including relation number, attribute number, type information, and nesting level. For simplicity, some advanced fields like varnullingrels are initialized to default values and can be modified later if needed.

## Parameters
- `varno`: Integer index of the variable's relation in the range table, or special values like INNER_VAR/OUTER_VAR
- `varattno`: AttrNumber representing the attribute number of this variable, or zero for whole-row references
- `vartype`: Oid of the PostgreSQL type (from pg_type) for this variable
- `vartypmod`: int32 type modifier value from pg_attribute, providing additional type information
- `varcollid`: Oid of the collation for this variable, or InvalidOid if no specific collation
- `varlevelsup`: Index indicating nesting level for subquery variables (0 for normal variables, >0 for outer references)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for Var node allocation)
  - Var (struct type from primnodes.h)
  - AttrNumber, Oid, Index (basic type definitions)
- Called from (representative examples):
  - makeVarFromTargetEntry
  - makeWholeRowVar
  - buildVarFromNSColumn
  - expandRTE
  - transformAssignedExpr
  - get_qual_for_hash
  - rewriteSearchAndCycle

## Notes and Other Information
- Sets varnullingrels to NULL by default - callers must set this if outer join nullification is relevant
- Initializes varnosyn/varattnosyn to match varno/varattno for syntactic compatibility
- Sets location to -1 (unknown) by default - callers should update if source location is available
- Widely used throughout the parser, optimizer, and rewriter for creating column references
- Part of the core expression node construction utilities in makefuncs.c