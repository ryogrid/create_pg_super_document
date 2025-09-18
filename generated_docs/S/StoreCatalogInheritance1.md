# StoreCatalogInheritance1

## Location
src/backend/commands/tablecmds.c: 3433 - 3477

## Overview  
StoreCatalogInheritance1 creates a single inheritance relationship entry in the system catalogs, establishing the parent-child relationship between two relations along with proper dependencies and metadata updates.

## Definition


## Detailed Description
This function performs the atomic operation of recording a single inheritance relationship between a child and parent relation. It orchestrates several critical steps in inheritance setup:

1. **Catalog Entry Creation**: Stores the inheritance record in pg_inherits using StoreSingleInheritance
2. **Dependency Management**: Records dependency between child and parent relations for proper cascade behavior
3. **Hook Invocation**: Triggers post-creation hooks for inheritance events, allowing extensions to respond
4. **Parent Metadata Update**: Marks the parent relation as having subclasses for query planning optimization

The function handles both regular table inheritance and table partitioning scenarios, with appropriate dependency types determined by child_dependency_type().

## Parameters / Member Variables
- : OID of the child relation inheriting from the parent
- : OID of the parent relation being inherited from  
- : Sequence number indicating inheritance order (for multiple inheritance)
- : Already opened pg_inherits catalog relation handle
- : Boolean indicating if child is a partition (affects dependency type)

## Dependencies
- Functions called/Symbols referenced:
  - StoreSingleInheritance
  - recordDependencyOn
  - child_dependency_type  
  - InvokeObjectPostAlterHookArg
  - SetRelationHasSubclass
  - ObjectAddress (structure type)
- Called from (representative examples):
  - StoreCatalogInheritance
  - CreateInheritance

## Notes and Other Information
- Works with an already-opened pg_inherits catalog relation for efficiency
- Creates ObjectAddress structures for both parent and child relations with RelationRelationId class
- Uses auxiliary_id argument in hook invocation since object_access_hook doesn't support multiple object identifiers
- The seqNumber parameter maintains inheritance ordering which is important for multiple inheritance scenarios
- Dependencies ensure proper cascade behavior when parent relations are dropped
- Parent relation metadata update (relhassubclass) enables query planner optimizations for inheritance hierarchies