# TidStore

## Location
[src/backend/access/common/tidstore.c:114-131](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tidstore.c#L114-L131)

## Overview
TidStore is the main data structure for efficiently storing and managing collections of tuple identifiers (TIDs) in PostgreSQL, supporting both local and shared memory configurations through a radix tree-based implementation.

## Definition

```c
struct TidStore
{
	/* MemoryContext where the TidStore is allocated */
	MemoryContext context;

	/* MemoryContext that the radix tree uses */
	MemoryContext rt_context;

	/* Storage for TIDs. Use either one depending on TidStoreIsShared() */
	union
	{
		local_ts_radix_tree *local;
		shared_ts_radix_tree *shared;
	}			tree;

	/* DSA area for TidStore if using shared memory */
	dsa_area   *area;
};
```
## Detailed Description
TidStore is PostgreSQL's primary mechanism for storing collections of tuple identifiers efficiently. It uses a radix tree data structure as its underlying storage mechanism, which provides excellent performance characteristics for sparse TID sets. The structure is designed to work in both local memory (single-backend) and shared memory (multi-backend) scenarios. In local mode, it uses a standard memory context, while in shared mode, it leverages PostgreSQL's Dynamic Shared Area (DSA) infrastructure to enable cross-process access. This dual-mode design makes TidStore suitable for various use cases, from single-process operations like vacuum to parallel operations that require TID sharing between multiple worker processes.

## Parameters / Member Variables
- `context`: The MemoryContext where the TidStore structure itself is allocated
- `rt_context`: The MemoryContext used by the underlying radix tree for its internal allocations
- `tree.local`: Pointer to local radix tree when operating in single-backend mode
- `tree.shared`: Pointer to shared radix tree when operating in multi-backend shared memory mode
- `area`: DSA (Dynamic Shared Area) used for managing shared memory allocations when in shared mode

## Dependencies
- Functions called/Symbols referenced:
  - [dsa_area](../d/dsa_area.md) (data type for shared memory management)
- Called from (representative examples):
  - [TidStoreCreateLocal](TidStoreCreateLocal.md)
  - [TidStoreCreateShared](TidStoreCreateShared.md)
  - [TidStoreAttach](TidStoreAttach.md)
  - [TidStoreDetach](TidStoreDetach.md)
  - [TidStoreDestroy](TidStoreDestroy.md)
  - [TidStoreSetBlockOffsets](TidStoreSetBlockOffsets.md)
  - [TidStoreIsMember](TidStoreIsMember.md)
  - [TidStoreBeginIterate](TidStoreBeginIterate.md)

## Notes and Other Information
- The union design allows efficient switching between local and shared storage modes without code duplication
- Memory management is carefully designed with separate contexts for the TidStore structure and its radix tree storage
- Used extensively in PostgreSQL's vacuum operations, both for single-process and parallel vacuum scenarios
- The DSA area is only allocated and used when operating in shared memory mode
- Supports concurrent access patterns through appropriate locking mechanisms when in shared mode
- The radix tree implementation provides efficient sparse storage, making it suitable for large tables with selective tuple operations