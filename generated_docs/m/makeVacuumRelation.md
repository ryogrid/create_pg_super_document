# makeVacuumRelation

## Location
[src/backend/nodes/makefuncs.c:879-893](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/makefuncs.c#L879-L893)

## Overview
Creates a VacuumRelation node that encapsulates information about a table or relation to be processed by VACUUM or ANALYZE commands.

## Definition

```c
VacuumRelation *
makeVacuumRelation(RangeVar *relation, Oid oid, List *va_cols)
```
## Detailed Description
This function constructs a VacuumRelation structure used to represent a single table or relation that will be processed during VACUUM or ANALYZE operations. The VacuumRelation node serves as a container for the essential information needed to identify and process a specific relation during maintenance operations.

The structure supports both relation identification methods: by name (through RangeVar) for user-specified tables, and by OID for system-identified relations (such as during autovacuum operations). It also allows specification of particular columns for targeted ANALYZE operations.

## Parameters / Member Variables
- : RangeVar structure containing the schema-qualified name of the relation (can be NULL if identifying by OID)
- : Object ID of the relation (can be InvalidOid if identifying by name)
- : List of column names to be analyzed (for ANALYZE operations), or NIL for all columns

## Dependencies
- Functions called/Symbols referenced:
  -  - Creates a new node of type VacuumRelation
  -  - The vacuum relation node structure type
  -  - Structure representing a qualified relation name
- Called from (representative examples):
  -  - Expands relation specifications for vacuum operations
  -  - Retrieves all relations for database-wide vacuum
  -  - Autovacuum worker relation processing

## Notes and Other Information
- Essential component of PostgreSQL's VACUUM and ANALYZE infrastructure
- The function provides a simple wrapper around the three core fields needed for relation identification
- Supports both user-initiated maintenance commands and automated maintenance via autovacuum
- Used in command processing to maintain a list of relations that need vacuum/analyze operations
- The dual identification system (RangeVar vs OID) accommodates different operational contexts where relations may be specified differently

## Simplified Source

```c
VacuumRelation *
makeVacuumRelation(RangeVar *relation, Oid oid, List *va_cols)
{
    VacuumRelation *v = makeNode(VacuumRelation);

    v->relation = relation;  // Schema-qualified relation name
    v->oid = oid;           // Object ID for direct identification
    v->va_cols = va_cols;   // Column list for ANALYZE operations

    return v;
}
```