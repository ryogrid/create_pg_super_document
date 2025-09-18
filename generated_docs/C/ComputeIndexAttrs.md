# ComputeIndexAttrs

## Location
src/backend/commands/indexcmds.c: 1819 - 2192

## Overview
Computes per-index-column information including indexed column numbers, expressions, operator classes, and their options for all columns in an index definition.

## Definition


## Detailed Description
This function is a core component of index creation that processes the attribute list specification and translates it into the internal representation needed by PostgreSQL's index subsystem. It handles both simple column references and complex index expressions, validates data types and collations, resolves operator classes, and sets up exclusion operators for exclusion constraints. The function also handles included columns (non-key columns that are stored but not indexed) and validates various constraints specific to different access methods.

For each column/expression in the index:
1. Determines if it's a simple column reference or an expression
2. Validates the column exists (for simple columns) or expression is valid
3. Extracts type information and collation requirements
4. Resolves the appropriate operator class using ResolveOpClass
5. Sets up exclusion operators if this is an exclusion constraint
6. Configures column options like sort order and null handling
7. Handles security context switching for DDL operations

## Parameters / Member Variables
- : IndexInfo structure to populate with computed information
- : Output array of data type OIDs for each index column
- : Output array of collation OIDs for each index column
- : Output array of operator class OIDs for each index column
- : Output array of operator class options for each index column
- : Output array of column options (sort order, null handling) for each index column
- : Input list of IndexElem structures specifying the index columns/expressions
- : List of exclusion operator names (for exclusion constraints)
- : OID of the relation being indexed
- : Name of the index access method (btree, hash, etc.)
- : OID of the index access method
- : Whether the access method supports ordered indexes
- : Whether this index is being created for a constraint
- : User ID for DDL permission checks (InvalidOid if not needed)
- : Security context for DDL operations
- : Pointer to saved GUC nesting level for DDL operations

## Dependencies
- Functions called/Symbols referenced:
  - ResolveOpClass (for operator class resolution)
  - SearchSysCacheAttName (for column lookup)
  - exprType, exprCollation (for expression analysis)
  - get_collation_oid (for collation resolution)
  - compatible_oper_opid (for exclusion operator lookup)
  - contain_mutable_functions_after_planning (for expression validation)
  - type_is_collatable (for collation validation)
  - transformRelOptions (for operator class options)
- Called from (representative examples):
  - DefineIndex (main index creation function)
  - CheckIndexCompatible (index compatibility checking)

## Notes and Other Information
- Handles both key columns and included columns, with different validation rules
- Validates that expressions in included columns are not allowed
- Ensures exclusion operators are commutative and belong to the correct operator family
- Performs security context switching to handle DDL permissions properly
- Supports both simple column references and complex expressions as index keys
- Validates that mutable functions are not used in index expressions
- Sets up proper null ordering defaults based on sort direction for ordered access methods