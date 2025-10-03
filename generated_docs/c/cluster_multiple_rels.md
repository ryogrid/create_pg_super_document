# cluster_multiple_rels

## Location
[src/backend/commands/cluster.c:266-310](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/cluster.c#L266-L310)

## Overview
Processes a list of relations to be clustered, with each relation being clustered in its own separate transaction to avoid deadlock issues.

## Definition

```c
static void
cluster_multiple_rels(List *rtcs, ClusterParams *params)
```
## Detailed Description
The cluster_multiple_rels function implements the core logic for clustering multiple tables in separate transactions. This approach prevents deadlocks that could occur if exclusive locks were held on multiple tables simultaneously within a single transaction.

The function follows a specific transaction management pattern:
1. Commits the current transaction to exit the starting transaction context
2. Iterates through each relation in the provided list
3. For each relation, starts a new transaction, establishes a snapshot, performs the clustering operation, and commits

This design allows the CLUSTER command to process multiple tables safely while maintaining transactional integrity for each individual clustering operation.

## Parameters / Member Variables
- `*rtcs`: List of RelToCluster structures containing tableOid and indexOid pairs for each relation to be clustered
- `*params`: ClusterParams structure containing clustering options and configuration flags
## Dependencies
- Functions called/Symbols referenced:
  - [PopActiveSnapshot](../P/PopActiveSnapshot.md)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md)
  - [StartTransactionCommand](../S/StartTransactionCommand.md)
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md)
  - [PushActiveSnapshot](../P/PushActiveSnapshot.md)
  - [cluster_rel](cluster_rel.md)
- Called from (representative examples):
  - [cluster](cluster.md)

## Notes and Other Information
- The function expects to be called within an active transaction but leaves no transaction active upon return
- Each relation is processed independently, allowing partial success if some relations fail
- [Snapshot](../S/Snapshot.md) management is crucial for ensuring consistent reads within each transaction
- This pattern is similar to the approach used by VACUUM for processing multiple relations

## Simplified Source

```c
static void cluster_multiple_rels(List *rtcs, ClusterParams *params)
{
    ListCell *lc;

    // Exit the starting transaction
    PopActiveSnapshot();
    CommitTransactionCommand();

    // Process each relation in its own transaction
    foreach(lc, rtcs) {
        RelToCluster *rtc = (RelToCluster *) lfirst(lc);

        // Start new transaction for this relation
        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());

        // Perform the clustering operation
        cluster_rel(rtc->tableOid, rtc->indexOid, params);

        // Clean up and commit transaction
        PopActiveSnapshot();
        CommitTransactionCommand();
    }
}
```