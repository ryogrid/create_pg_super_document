# findFkeyCast

## Location
[src/backend/commands/tablecmds.c:12183-12211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L12183-L12211)

## Overview
A wrapper function around find_coercion_pathway() specifically designed for foreign key constraint validation that determines if type coercion is possible between source and target types.

## Definition

```c
static CoercionPathType
findFkeyCast(Oid targetTypeId, Oid sourceTypeId, Oid *funcid)
```
## Detailed Description
This function determines whether a type conversion (cast) is possible between a source type and target type in the context of foreign key constraints. It serves as a specialized wrapper around the general find_coercion_pathway() function, treating binary coercibility and exact type matches with equal preference. The function handles two scenarios: when types are identical (requiring only relabeling) and when types differ (requiring implicit coercion). If no valid coercion path exists, it raises an error indicating that a previously available cast is no longer available.

## Parameters / Member Variables
- : OID of the target data type (typically from referenced table column)
- : OID of the source data type (typically from referencing table column) 
- : Output parameter that receives the OID of the coercion function, or InvalidOid if no function needed

## Dependencies
- Functions called/Symbols referenced:
  - [find_coercion_pathway](find_coercion_pathway.md)
  - COERCION_PATH_RELABELTYPE
  - COERCION_IMPLICIT
  - COERCION_PATH_NONE
- Called from (representative examples):
  - [ATAddForeignKeyConstraint](../A/ATAddForeignKeyConstraint.md)

## Notes and Other Information
- Returns COERCION_PATH_RELABELTYPE for identical types (no actual conversion needed)
- Only accepts implicit coercion paths, not explicit ones
- Raises an ERROR if no coercion pathway exists, suggesting a regression in cast availability
- Part of the foreign key constraint validation process during table alterations
- The error message indicates this function expects previously working casts to remain available

## Simplified Source

```c
static CoercionPathType
findFkeyCast(Oid targetTypeId, Oid sourceTypeId, Oid *funcid)
{
    CoercionPathType ret;

    if (targetTypeId == sourceTypeId)
    {
        // Types are identical - just relabel
        ret = COERCION_PATH_RELABELTYPE;
        *funcid = InvalidOid;
    }
    else
    {
        // Types differ - find implicit coercion path
        ret = find_coercion_pathway(targetTypeId, sourceTypeId,
                                   COERCION_IMPLICIT, funcid);
        if (ret == COERCION_PATH_NONE)
            elog(ERROR, "could not find cast from %u to %u",
                 sourceTypeId, targetTypeId);
    }

    return ret;
}
```