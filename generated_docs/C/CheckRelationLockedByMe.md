# CheckRelationLockedByMe

## Location
src/backend/storage/lmgr/lmgr.c: 330 - 346

## Overview
CheckRelationLockedByMe checks whether the current transaction holds a lock on the specified relation with the given lock mode or potentially stronger.

## Definition


## Detailed Description
This function verifies if the current transaction has acquired a lock on the specified relation. It constructs a lock tag from the relation's database and relation identifiers, then delegates to LockHeldByMe to perform the actual lock check. The function can optionally check for stronger lock modes when the orstronger parameter is true, where "stronger" is defined numerically (higher LOCKMODE values).

## Parameters / Member Variables
- : The relation to check for lock ownership
- : The minimum lock mode to check for  
- : If true, also accepts stronger (numerically higher) lock modes as satisfying the check

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_RELATION (macro to construct relation lock tag)
  - LockHeldByMe (performs the actual lock ownership check)
- Called from (representative examples):
  - relation_open
  - try_relation_open
  - addFkRecurseReferenced
  - addFkRecurseReferencing
  - ExecGetRangeTableRelation

## Notes and Other Information
- Returns true if the current transaction holds the specified lock or stronger
- Uses the relation's lockRelId which contains both database ID and relation ID
- The "stronger" lock concept is semantically questionable but works for its intended purposes
- Located in src/backend/storage/lmgr/lmgr.c:330-346