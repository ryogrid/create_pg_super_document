# vacuum_get_cutoffs

## Location
[src/backend/commands/vacuum.c:1083-1250](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuum.c#L1083-L1250)

## Overview
Computes OldestXmin and freeze cutoff points for vacuum operations, determining whether an aggressive vacuum is needed based on transaction age thresholds.

## Definition
```c
bool
vacuum_get_cutoffs(Relation rel, const VacuumParams *params,
                   struct VacuumCutoffs *cutoffs)
```

## Detailed Description
The vacuum_get_cutoffs function calculates critical cutoff values that determine how vacuum operations should behave. It computes OldestXmin (oldest transaction that cannot be removed), freeze limits for both regular transactions and multixacts, and determines whether an aggressive vacuum is required.

The function considers various age parameters and system limits to balance performance with wraparound protection. It generates warnings when cutoffs are dangerously far in the past and ensures that computed limits don't exceed safe boundaries. The return value indicates whether the vacuum should be aggressive, meaning it must advance relfrozenxid and relminmxid to prevent transaction ID wraparound.

## Parameters / Member Variables
- `rel`: Target relation for the vacuum operation
- `params`: VACUUM parameters containing freeze age settings and other options
- `cutoffs`: Output structure to be filled with computed cutoff values including OldestXmin, FreezeLimit, MultiXactCutoff, etc.

## Dependencies
- Functions called/Symbols referenced:
  - [GetOldestNonRemovableTransactionId](../G/GetOldestNonRemovableTransactionId.md)
  - [GetOldestMultiXactId](../G/GetOldestMultiXactId.md)
  - [ReadNextTransactionId](../R/ReadNextTransactionId.md)
  - [ReadNextMultiXactId](../R/ReadNextMultiXactId.md)
  - [MultiXactMemberFreezeThreshold](../M/MultiXactMemberFreezeThreshold.md)
  - TransactionIdIsNormal
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - [TransactionIdPrecedesOrEquals](../T/TransactionIdPrecedesOrEquals.md)
  - [MultiXactIdPrecedes](../M/MultiXactIdPrecedes.md)
  - [MultiXactIdPrecedesOrEquals](../M/MultiXactIdPrecedesOrEquals.md)
- Called from (representative examples):
  - [heap_vacuum_rel](../h/heap_vacuum_rel.md) (src/backend/access/heap/vacuumlazy.c:449)
  - [copy_table_data](../c/copy_table_data.md) (src/backend/commands/cluster.c:916)

## Notes and Other Information
- Returns true if aggressive vacuum is needed, false for non-aggressive vacuum
- Issues warnings when transaction cutoffs are dangerously old
- Considers both regular transaction IDs and multixact IDs for comprehensive wraparound protection
- Uses configuration parameters like autovacuum_freeze_max_age and vacuum_freeze_min_age
- Computes effective freeze thresholds based on available multixact member space
- Ensures computed limits never exceed safe boundaries to prevent wraparound issues
- Location: src/backend/commands/vacuum.c:1083-1250