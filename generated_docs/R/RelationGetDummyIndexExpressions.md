# RelationGetDummyIndexExpressions

## Location
[src/backend/utils/cache/relcache.c:5102-5155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L5102-L5155)

## Overview
RelationGetDummyIndexExpressions creates dummy expressions (NULL constants) that match the types, type modifiers, and collations of an index's real expressions, avoiding execution of user-defined code.

## Definition
```c
List *RelationGetDummyIndexExpressions(Relation relation)
```

## Detailed Description
This function provides a safe way to obtain type-compatible placeholders for index expressions without executing potentially dangerous or expensive user-defined code. It parses the stored index expressions to extract their type information, then constructs NULL constant nodes with matching type characteristics.

The function operates through these steps:
1. Returns NIL immediately if the relation has no index expressions
2. Retrieves the raw expression string from pg_index.indexprs
3. Parses the string into a raw expression tree using stringToNode
4. For each expression in the tree, extracts its type, typmod, and collation information
5. Creates a NULL Const node with matching type characteristics
6. Returns the list of dummy constants

This approach is particularly useful in scenarios where you need type information about index expressions but cannot safely execute the actual expressions, such as during certain catalog operations or in error recovery situations.

## Parameters / Member Variables
- `relation`: The index relation for which to create dummy expressions

## Dependencies
- Functions called/Symbols referenced:
  - [heap_attisnull](../h/heap_attisnull.md)
  - [heap_getattr](../h/heap_getattr.md)
  - [GetPgIndexDescriptor](../G/GetPgIndexDescriptor.md)
  - TextDatumGetCString
  - [stringToNode](../s/stringToNode.md)
  - [makeConst](../m/makeConst.md)
  - exprTypmod
  - [exprCollation](../e/exprCollation.md)
- Called from (representative examples):
  - [BuildDummyIndexInfo](../B/BuildDummyIndexInfo.md)

## Notes and Other Information
- Returns NIL for relations without index expressions
- Creates NULL constants with isnull=true and constbyval=true
- The typlen parameter is set to 1 arbitrarily since the constant is always NULL
- Does not cache results unlike RelationGetIndexExpressions
- Provides type safety without expression evaluation risks
- Essential for operations that need type information but cannot execute user code
- Located in src/backend/utils/cache/relcache.c:5102-5155