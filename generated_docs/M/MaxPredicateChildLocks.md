# MaxPredicateChildLocks

## Location
[src/backend/storage/lmgr/predicate.c:2279-2315](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L2279-L2315)

## Overview
Returns the promotion limit for a given predicate lock target, specifying the maximum number of descendant locks allowed before promoting to the specified tag.

## Definition


## Detailed Description
MaxPredicateChildLocks is a static function in PostgreSQL's predicate locking system that determines the threshold for lock promotion. It returns the maximum number of child (descendant) locks that can exist before the system should promote them to a coarser-grained lock represented by the given tag. This mechanism prevents lock proliferation and ensures efficient memory usage in the serializable snapshot isolation system.

The function implements different promotion limits based on the lock granularity level:
- For relation locks: Uses either a configured value or calculates a default based on max_predicate_locks_per_xact
- For page locks: Uses the configured max_predicate_locks_per_page value (typically 2)
- For tuple locks: Should never be reached since tuples are the finest granularity

The limit includes both direct and indirect descendants (e.g., for a relation lock, both page locks and tuple locks count toward the limit). This design helps maintain a balanced allocation of locks and prevents any single relation from consuming all predicate lock resources.

## Parameters / Member Variables
- : Pointer to a PREDICATELOCKTARGETTAG that specifies the lock target type and granularity for which to determine the promotion limit.

## Dependencies
- Functions called/Symbols referenced:
  - GET_PREDICATELOCKTARGETTAG_TYPE
  - PREDLOCKTAG_RELATION
  - PREDLOCKTAG_PAGE  
  - PREDLOCKTAG_TUPLE
  - max_predicate_locks_per_relation (GUC variable)
  - max_predicate_locks_per_xact (GUC variable)
  - max_predicate_locks_per_page (GUC variable)
- Called from (representative examples):
  - [SerialControl](../S/SerialControl.md)
  - [CheckAndPromotePredicateLockRequest](../C/CheckAndPromotePredicateLockRequest.md)

## Notes and Other Information
- This is a static function only accessible within the predicate.c file
- The function includes TODO comments suggesting future enhancements for more sophisticated lock allocation strategies
- Default limit for page locks is typically 2
- [Relation](../R/Relation.md) lock limit defaults to (max_predicate_locks_per_xact / (-max_predicate_locks_per_relation)) - 1 when max_predicate_locks_per_relation is negative
- The function asserts that tuple-level promotion should never be requested since tuples are the finest granularity
- Part of PostgreSQL's lock escalation strategy to prevent memory exhaustion
- The promotion thresholds are configurable via GUC (Grand Unified Configuration) parameters
- Future improvements may include ratio-based limits relative to actual page/tuple counts in relations