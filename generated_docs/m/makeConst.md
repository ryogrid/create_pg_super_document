# makeConst

## Location
src/backend/nodes/makefuncs.c: 348 - 385

## Overview
Creates a Const node representing a constant value in PostgreSQL's expression tree, handling proper storage format and type information.

## Definition
```c
Const *makeConst(Oid consttype, int32 consttypmod, Oid constcollid, int constlen, 
                 Datum constvalue, bool constisnull, bool constbyval)
```

## Detailed Description
The `makeConst` function allocates and initializes a new Const node, which represents a constant value in PostgreSQL's expression system. Const nodes are fundamental building blocks used throughout query processing to represent literal values like numbers, strings, dates, etc.

The function performs important normalization by ensuring variable-length (varlena) values are stored in non-expanded, non-toasted format. This eliminates dependencies on external values and ensures consistent representation for equality comparisons, which is crucial for query optimization and plan caching.

## Parameters / Member Variables
- `consttype`: OID of the constant's data type
- `consttypmod`: Type modifier (additional type information like precision/scale)
- `constcollid`: OID of the collation for collatable types (InvalidOid if not applicable)
- `constlen`: Length of the data type (-1 for variable-length types, positive for fixed-length)
- `constvalue`: The actual constant value as a Datum (PostgreSQL's generic value representation)
- `constisnull`: Boolean flag indicating whether the constant represents a NULL value
- `constbyval`: Boolean flag indicating whether the value is passed by value (true) or by reference (false)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (macro for node allocation)
  - PG_DETOAST_DATUM (macro for detoasting varlena values)
  - PointerGetDatum (macro for converting pointer to Datum)
  - Const (node type being created)
- Called from (representative examples):
  - makeNullConst (same file)
  - makeBoolConst (same file)
  - make_const (parser)
  - eval_const_expressions_mutator (optimizer)
  - build_coercion_expression (parser)

## Notes and Other Information
- Automatically detoasts varlena values (constlen == -1) to ensure consistent representation
- Sets location field to -1 ("unknown") by default - can be updated by parser if source location is known
- Critical for constant folding during query optimization
- Used extensively throughout the system for representing literal values in expressions
- The detoasting behavior ensures that equal constants have identical internal representation
- Located in src/backend/nodes/makefuncs.c:348-385