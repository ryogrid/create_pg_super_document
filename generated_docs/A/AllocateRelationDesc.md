# AllocateRelationDesc

## Location
src/backend/utils/cache/relcache.c: 409 - 463

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
  - RelationData (relation descriptor structure type)
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