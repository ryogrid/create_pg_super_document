# AllocateRelationDesc

## Location
[src/backend/utils/cache/relcache.c:409-463](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L409-L463)

## Overview
AllocateRelationDesc allocates memory for a new relation descriptor and initializes its basic structure from a pg_class tuple, serving as the foundation for relation cache entries.

## Definition
static Relation AllocateRelationDesc(Form_pg_class relp)

## Detailed Description
AllocateRelationDesc is responsible for creating the basic memory structure of a relation descriptor (RelationData) in the cache memory context. The function allocates space for the relation descriptor, copies the fixed-size portion of the pg_class tuple form, and initializes the tuple descriptor for attributes. It carefully manages memory contexts to ensure relcache entries are allocated in the persistent CacheMemoryContext.

The function creates a template tuple descriptor using CreateTemplateTupleDesc and sets up reference counting for proper memory management. Variable-length fields like relacl and reloptions are explicitly not stored in the relcache structure for efficiency reasons, as noted in the extensive comments.

## Parameters / Member Variables
- : Pointer to a Form_pg_class structure containing the pg_class tuple data to copy into the relation descriptor

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_class (pg_class tuple structure type)
  - [RelationData](../R/RelationData.md) (relation descriptor structure type)
  - CLASS_TUPLE_SIZE (constant for fixed-size portion of pg_class)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md) (creates empty tuple descriptor template)
- Called from (representative examples):
  - [RelationBuildDesc](../R/RelationBuildDesc.md) (main relation descriptor building function)

## Notes and Other Information
- Allocates memory in CacheMemoryContext to ensure proper lifetime management
- Only copies the fixed-size portion (CLASS_TUPLE_SIZE) of pg_class tuples
- [Variable](../V/Variable.md)-length fields (relacl, reloptions) are intentionally excluded from relcache storage
- Sets up reference counting (tdrefcount = 1) for the tuple descriptor
- Initializes rd_smgr to NULL to indicate no open storage manager file
- The returned relation descriptor requires further initialization by other functions
- Critical for relcache memory management and ensuring consistent relation metadata access

## Simplified Source

```c
static Relation
AllocateRelationDesc(Form_pg_class relp)
{
    // Switch to cache memory context for persistent storage
    MemoryContext oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

    // Allocate and zero-initialize relation descriptor
    Relation relation = (Relation) palloc0(sizeof(RelationData));

    // Initialize storage manager reference to NULL
    relation->rd_smgr = NULL;

    // Copy the fixed-size portion of pg_class tuple
    // Note: Variable-length fields (relacl, reloptions) are not copied
    Form_pg_class relationForm = (Form_pg_class) palloc(CLASS_TUPLE_SIZE);
    memcpy(relationForm, relp, CLASS_TUPLE_SIZE);

    // Set relation tuple form
    relation->rd_rel = relationForm;

    // Create template tuple descriptor for attributes
    relation->rd_att = CreateTemplateTupleDesc(relationForm->relnatts);

    // Mark tuple descriptor as reference-counted
    relation->rd_att->tdrefcount = 1;

    MemoryContextSwitchTo(oldcxt);

    return relation;
}
```