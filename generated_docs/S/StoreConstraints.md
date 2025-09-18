# StoreConstraints

## Location
[src/backend/catalog/heap.c:2240-2313](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L2240-L2313)

## Overview
StoreConstraints processes a list of pre-cooked constraints and defaults, storing them in the appropriate system catalogs for a relation.

## Definition


## Detailed Description
StoreConstraints is a static function that takes a list of CookedConstraint structures and stores them in PostgreSQL's system catalogs. The function handles two types of constraints: DEFAULT constraints (column defaults) and CHECK constraints. Each CookedConstraint struct is modified to store the new catalog tuple OID after successful creation.

The function is specifically designed to handle pre-cooked expressions that are inherited from existing relations, not newly parsed expressions. The function ensures proper visibility of pg_attribute tuples by incrementing the command counter before processing constraints, which triggers a relcache entry rebuild.

Key operations include:
1. Incrementing the command counter to ensure pg_attribute tuples are visible
2. Iterating through the list of cooked constraints
3. Dispatching to appropriate storage functions based on constraint type
4. Updating the relation's check constraint count if any check constraints were added

## Parameters / Member Variables
- : The relation for which constraints are being stored
- : A list of CookedConstraint structures containing pre-processed constraint information
- : Whether these constraints are internally constructed by the system

## Dependencies
- Functions called/Symbols referenced:
  - CommandCounterIncrement
  - CookedConstraint
  - CONSTR_DEFAULT
  - [StoreAttrDefault](StoreAttrDefault.md)
  - CONSTR_CHECK
  - [StoreRelCheck](StoreRelCheck.md)
  - [SetRelationNumChecks](SetRelationNumChecks.md)
- Called from (representative examples):
  - [heap_create_with_catalog](../h/heap_create_with_catalog.md)

## Notes and Other Information
- Only processes pre-cooked expressions inherited from existing relations; newly parsed expressions should use direct calls to StoreAttrDefault and StoreRelCheck
- The function modifies each CookedConstraint struct to store the new catalog tuple OID
- Requires a command counter increment to ensure pg_attribute tuples are visible for constraint deparsing
- Automatically updates the relation's check constraint count when check constraints are processed
- Returns void but modifies the constraint structures in-place with their new OIDs
- The function will error on unrecognized constraint types