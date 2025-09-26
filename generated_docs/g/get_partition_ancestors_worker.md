# get_partition_ancestors_worker

## Location
[src/backend/catalog/partition.c:153-175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/partition.c#L153-L175)

## Overview
A recursive static helper function that builds the list of ancestors for a given partition relation.

## Definition
```c
static void get_partition_ancestors_worker(Relation inhRel, Oid relid, List **ancestors)
```

## Detailed Description
This function implements the recursive logic for building the ancestor list. It starts from a given relation and walks up the partition hierarchy by repeatedly calling `get_partition_parent_worker` to find each parent. For each valid parent found, it appends the parent OID to the ancestors list and then recursively calls itself with the parent OID to continue up the hierarchy.

The recursion terminates when either no parent is found (topmost level reached) or when the partition is being detached. The function modifies the ancestors list in-place by adding each parent OID it discovers.

## Parameters / Member Variables
- `inhRel`: An already-opened Relation object for the pg_inherits catalog table
- `relid`: OID of the current partition relation being processed
- `ancestors`: Pointer to a List pointer that accumulates the ancestor OIDs

## Dependencies
- Functions called/Symbols referenced:
  - [get_partition_parent_worker](get_partition_parent_worker.md) (to find the immediate parent)
  - [lappend_oid](../l/lappend_oid.md) (to append parent OID to the ancestors list)
  - [get_partition_ancestors_worker](get_partition_ancestors_worker.md) (recursive self-call)

- Called from (representative examples):
  - [get_partition_ancestors](get_partition_ancestors.md)
  - [get_partition_ancestors_worker](get_partition_ancestors_worker.md) (recursive call)

## Notes and Other Information
- Static function, only accessible within partition.c
- Uses tail recursion to build the ancestor list from bottom to top
- The ancestors list is built in order: immediate parent first, topmost parent last
- Terminates recursion when parentOid is InvalidOid or when detach_pending is true
- Located at src/backend/catalog/partition.c:153-175
- The function modifies the ancestors list passed by reference