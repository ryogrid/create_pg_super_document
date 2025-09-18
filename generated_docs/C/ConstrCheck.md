# ConstrCheck

## Location
src/include/access/tupdesc.h: 28 - 34

## Overview
ConstrCheck represents a CHECK constraint in PostgreSQL's constraint system, storing the constraint name, expression, validity status, and inheritance properties.

## Definition


## Detailed Description
ConstrCheck is a structure that stores information about CHECK constraints on database tables. CHECK constraints are user-defined conditions that must be satisfied by all rows in a table. This structure is part of PostgreSQL's constraint management system and is used within tuple descriptors to maintain CHECK constraint information.

The structure stores the constraint name, a serialized representation of the constraint expression, and metadata about the constraint's validity and inheritance behavior. The constraint expression is stored in a nodeToString format, allowing complex boolean expressions to be preserved and later evaluated.

## Parameters / Member Variables
- : String containing the name of the CHECK constraint
- : String containing the nodeToString representation of the constraint expression
- : Boolean indicating whether the constraint is currently valid/enabled
- : Boolean indicating whether this constraint should not be inherited by child tables

## Dependencies
- Functions called/Symbols referenced:
  - (primitive types only)
- Called from (representative examples):
  - CreateTupleDescCopyConstr
  - FreeTupleDesc
  - equalTupleDescs
  - MergeAttributes
  - ExecRelCheck
  - CheckConstraintFetch
  - CheckConstraintCmp

## Notes and Other Information
- CHECK constraints are evaluated during INSERT and UPDATE operations to ensure data integrity
- The ccvalid field allows constraints to be temporarily disabled without dropping them
- The ccnoinherit field controls inheritance behavior in table hierarchies
- ConstrCheck structures are typically stored in arrays within TupleConstr
- The ccbin field stores expressions that can be reconstructed using stringToNode()
- Used extensively in constraint validation and table inheritance operations