# RelationBuildRowSecurity

## Location
[src/backend/commands/policy.c:193-331](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/policy.c#L193-L331)

## Overview
Loads row-level security policies from the system catalog (pg_policy) and builds the in-memory row security descriptor structure that gets cached in the relation's relcache entry.

## Definition
```c
void RelationBuildRowSecurity(Relation relation)
```

## Detailed Description
This function is responsible for constructing the complete row security policy information for a relation by scanning the pg_policy system catalog and building an in-memory representation. The function performs several key operations:

1. **Memory Context Management**: Creates a dedicated memory context ("row security descriptor") for all policy-related data, enabling efficient cleanup during relcache flushes
2. **Policy Discovery**: Scans pg_policy using the (polrelid, polname) index to consistently retrieve policies in name order
3. **Policy Parsing**: For each policy found, extracts and parses:
   - Command type (SELECT, INSERT, UPDATE, DELETE, or ALL)
   - Permissive vs restrictive policy type
   - Policy name
   - Applicable roles (converted from Datum array)
   - USING clause (qual expression)
   - WITH CHECK clause (with_check_qual expression)
4. **Expression Analysis**: Determines if policies contain sublinks for optimization purposes
5. **Cache Integration**: Attaches the completed descriptor to the relation's relcache entry

The function ensures proper memory management by carefully switching memory contexts when allocating pass-by-reference data and reparenting the final context under CacheMemoryContext for persistence.

## Parameters / Member Variables
- `relation`: Relation structure for which to build row security policy information

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate (memory context creation)
  - MemoryContextCopyAndSetIdentifier (context identification)
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md) (zero-initialized allocation)
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md) (string duplication in context)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (context switching)
  - [MemoryContextSetParent](../M/MemoryContextSetParent.md) (context reparenting)
  - [table_open](../t/table_open.md) (catalog access)
  - [table_close](../t/table_close.md) (catalog cleanup)
  - [ScanKeyInit](../S/ScanKeyInit.md) (scan key initialization)
  - [systable_beginscan](../s/systable_beginscan.md) (system catalog scan)
  - [systable_getnext](../s/systable_getnext.md) (scan iteration)
  - [systable_endscan](../s/systable_endscan.md) (scan cleanup)
  - [heap_getattr](../h/heap_getattr.md) (tuple attribute extraction)
  - DatumGetArrayTypePCopy (array datum processing)
  - TextDatumGetCString (text datum conversion)
  - [stringToNode](../s/stringToNode.md) (expression parsing)
  - [checkExprHasSubLink](../c/checkExprHasSubLink.md) (sublink detection)
  - [lcons](../l/lcons.md) (list construction)
  - RelationGetRelid (relation OID extraction)
  - RelationGetRelationName (relation name extraction)

- Called from:
  - [RelationBuildDesc](RelationBuildDesc.md) (during relation cache building)
  - Critical system index operations

## Notes and Other Information
- This is a public function, accessible from other PostgreSQL modules
- Assumes the caller has verified that pg_class.relrowsecurity is true for the relation
- Uses the PolicyPolrelidPolnameIndexId index for efficient policy lookup
- Policies are stored in reverse order in the descriptor list for historical reasons
- The function handles both USING and WITH CHECK clauses, which may be null
- Memory context management ensures that policy data persists across transaction boundaries while being cleanly freed during relcache invalidation
- Expression parsing converts stored text representations back into executable expression trees
- The hassublinks flag optimization helps the planner make informed decisions about policy evaluation costs
- Proper error handling is included for unexpected null values in required policy fields