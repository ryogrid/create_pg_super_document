# RowSecurityDesc

## Location
[src/include/rewrite/rowsecurity.h:31-35](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/rewrite/rowsecurity.h#L31-L35)

## Overview
RowSecurityDesc is a container structure that holds all row-level security policies for a specific relation, providing both memory management and organized access to the collection of security policies.

## Definition

```c
typedef struct RowSecurityDesc
{
	MemoryContext rscxt;		/* row security memory context */
	List	   *policies;		/* list of row security policies */
} RowSecurityDesc;
```
## Detailed Description
RowSecurityDesc serves as the top-level container for all row-level security information associated with a database relation. It encapsulates both the memory management context used for allocating policy-related data structures and a list containing all RowSecurityPolicy objects that apply to the relation.

This structure is typically stored in the relation cache (RelationData) and is built when row security policies are first needed for a relation. The memory context ensures that all policy-related allocations can be efficiently managed and freed together when the relation cache entry is invalidated or rebuilt.

## Parameters / Member Variables
- `rscxt`: Memory context specifically allocated for row security data structures, ensuring proper memory lifecycle management for all policy-related allocations
- `*policies`: Linked list containing RowSecurityPolicy structures that define the actual security rules for the relation
## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContext](../M/MemoryContext.md) (for memory management)
  - [List](../L/List.md) (PostgreSQL's linked list implementation)
  - [RowSecurityPolicy](RowSecurityPolicy.md) (contained within the policies list)

- Called from (representative examples):
  - [RelationBuildRowSecurity](RelationBuildRowSecurity.md) (policy.c:197, 215)
  - [equalRSDesc](../e/equalRSDesc.md) (relcache.c:999)
  - SWAPFIELD (relcache.c:2794)
  - [RelationData](RelationData.md).rd_rsdesc (rel.h:119)

## Notes and Other Information
- Each relation can have at most one RowSecurityDesc, but that descriptor can contain multiple RowSecurityPolicy objects
- The structure is built lazily - only created when row security is actually needed for a relation
- Memory allocated in rscxt includes the policy expressions, role arrays, and other policy-related data
- The policies list is ordered and may be sorted by policy name or other criteria for consistent application
- When relation cache entries are invalidated, the entire RowSecurityDesc and its memory context are freed together
- This structure is part of the relation cache infrastructure and benefits from PostgreSQL's cache invalidation mechanisms
- The separation of memory context from the policy list allows for efficient bulk memory management while maintaining list operations