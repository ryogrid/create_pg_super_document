# StoreRelCheck

## Location
src/backend/catalog/heap.c: 2130 - 2239

## Overview
StoreRelCheck stores a check constraint expression for a relation in PostgreSQL's system catalogs and returns the OID of the newly created constraint.

## Definition


## Detailed Description
StoreRelCheck is a static function that creates and stores a check constraint for a given relation. The function converts the constraint expression to string form for storage, analyzes the expression to identify which columns are referenced, and creates the constraint entry in the pg_constraint catalog. The function handles the complete process of constraint creation including validation of constraint properties like NO INHERIT constraints on partitioned tables.

The function performs several key operations:
1. Flattens the expression tree to string form using nodeToString()
2. Analyzes the expression to extract referenced column attributes using pull_var_clause()
3. Removes duplicate column references to create a unique list of affected columns
4. Validates constraint properties (e.g., NO INHERIT constraints cannot be added to partitioned tables)
5. Creates the constraint entry via CreateConstraintEntry() with all necessary metadata

## Parameters / Member Variables
- : The relation to which the check constraint is being added
- : The name of the check constraint
- : The constraint expression as a Node tree
- : Whether the constraint is initially validated
- : Whether the constraint is locally defined (not inherited)
- : Inheritance count for the constraint
- : Whether the constraint should not be inherited by child tables
- : Whether the constraint is internally constructed by the system

## Dependencies
- Functions called/Symbols referenced:
  - nodeToString
  - pull_var_clause
  - CreateConstraintEntry
  - RelationGetNamespace
  - CONSTRAINT_CHECK
- Called from (representative examples):
  - StoreConstraints
  - AddRelationNewConstraints

## Notes and Other Information
- The caller is responsible for updating the constraint count in the pg_class entry for the relation
- The function includes validation to prevent NO INHERIT constraints on partitioned tables since they contain no rows themselves
- The function handles duplicate column references by maintaining a unique list of attribute numbers
- Memory allocated for the binary form of the constraint (ccbin) is properly freed after use
- Returns the OID of the newly created constraint for reference by calling code