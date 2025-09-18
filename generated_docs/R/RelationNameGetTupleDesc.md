# RelationNameGetTupleDesc

## Location
[src/backend/utils/fmgr/funcapi.c:1870-1902](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/funcapi.c#L1870-L1902)

## Overview
Creates a tuple descriptor by looking up and copying the structure of an existing relation (table) by name.

## Definition
```c
TupleDesc RelationNameGetTupleDesc(const char *relname)
```

## Detailed Description
This function takes a relation name (which can be schema-qualified) and returns a copy of that relation's tuple descriptor. It performs the full process of parsing the relation name, opening the relation with appropriate locking, copying the tuple descriptor, and properly closing the relation.

The function uses the standard PostgreSQL relation lookup mechanism, supporting both simple and qualified relation names (e.g., "mytable" or "myschema.mytable"). It acquires an AccessShareLock on the relation to ensure the tuple descriptor remains stable during the copy operation.

Note that while this function works as advertised, it's primarily maintained for backwards compatibility. Modern code typically uses more direct approaches for building tuple descriptors for function result types.

## Parameters / Member Variables
- `relname`: A null-terminated string containing the relation name, which may be schema-qualified (e.g., "schema.table")

## Dependencies
- Functions called/Symbols referenced:
  - [stringToQualifiedNameList](../s/stringToQualifiedNameList.md)
  - [makeRangeVarFromNameList](../m/makeRangeVarFromNameList.md)
  - [relation_openrv](../r/relation_openrv.md)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md)
  - RelationGetDescr
  - [relation_close](../r/relation_close.md)
  - [RangeVar](RangeVar.md) (type)
  - [Relation](Relation.md) (type)
  - [List](../L/List.md) (type)
- Called from (representative examples):
  - TypeFuncClass

## Notes and Other Information
- Acquires and releases AccessShareLock on the target relation for safe tuple descriptor access
- Returns a copy of the tuple descriptor, not the original, so the caller owns the returned memory
- Supports both simple relation names and schema-qualified names
- Primarily maintained for backwards compatibility with existing user code
- The returned tuple descriptor should be freed by the caller when no longer needed
- Will raise an error if the specified relation does not exist or cannot be accessed