# expandRelation

## Location
[src/backend/parser/parse_relation.c:3017-3041](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L3017-L3041)

## Overview
A helper function for expandRTE that handles the expansion of ordinary table relations by opening the relation, extracting its tuple descriptor, and delegating to expandTupleDesc for column processing.

## Definition


## Detailed Description
This function serves as a specialized subroutine of expandRTE that specifically handles the expansion of RTE_RELATION type entries. It follows a simple but critical pattern:

1. **Relation Access**: Opens the relation using its OID with AccessShareLock to prevent concurrent schema changes
2. **Tuple Descriptor Extraction**: Retrieves the relation's tuple descriptor (rd_att) which contains metadata about all columns
3. **Delegation**: Passes the tuple descriptor to expandTupleDesc for the actual column expansion work
4. **Cleanup**: Properly closes the relation and releases the lock

This design encapsulates the relation access logic and provides a clean interface between the RTE expansion system and the tuple descriptor expansion functionality. The function ensures proper locking discipline by acquiring and releasing AccessShareLock, which is appropriate for read-only metadata access.

## Parameters / Member Variables
- : Object identifier of the relation to expand, used to locate and open the relation
- : Alias information for the relation, containing alternative column names if specified
- : Range table index to use in created Var nodes, identifying this relation in the query context
- : Nesting level for Var nodes, indicating how many subquery levels up this relation is referenced
- : Source location information for error reporting and debugging purposes
- : Boolean flag determining whether to include dropped columns in the expansion
- : Output parameter for list of column name strings (pass NULL if not needed)
- : Output parameter for list of Var nodes representing columns (pass NULL if not needed)

## Dependencies
- Functions called/Symbols referenced:
  - [relation_open](../r/relation_open.md) (opens relation with specified lock)
  - [relation_close](../r/relation_close.md) (closes relation and releases lock)
  - [expandTupleDesc](expandTupleDesc.md) (performs actual tuple descriptor expansion)
- Data structures used:
  - [Relation](../R/Relation.md) (relation descriptor)
  - [Alias](../A/Alias.md) (alias information)
  - [TupleDesc](../T/TupleDesc.md) (accessed via rel->rd_att)
- Called from:
  - [expandRTE](expandRTE.md) (for RTE_RELATION case processing)

## Notes and Other Information
- Uses AccessShareLock which is the lightest lock level, appropriate for reading metadata without blocking concurrent operations
- The function is static, indicating it's an internal helper within parse_relation.c
- Follows PostgreSQL's standard pattern of acquiring locks before accessing relation metadata and properly releasing them
- The rd_att->natts parameter passed to expandTupleDesc represents the total number of attributes in the relation
- The '0' parameter in the expandTupleDesc call indicates starting from the first column (no offset)
- Critical for ensuring that relation column expansion is done safely with proper concurrency control
- Part of the relation expansion pipeline that connects high-level RTE processing with low-level tuple descriptor analysis