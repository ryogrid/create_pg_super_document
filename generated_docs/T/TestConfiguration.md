# TestConfiguration

## Location
src/backend/storage/lmgr/deadlock.c: 375 - 442

## Overview
Tests a configuration of constraints for validity, detecting and classifying deadlock cycles as hard, soft, or non-existent.

## Definition
static int TestConfiguration(PGPROC *startProc)

## Detailed Description
TestConfiguration evaluates the current set of constraints to determine if they represent a valid, deadlock-free lock configuration. The function expands the constraint set into wait orderings and systematically checks for deadlock cycles involving the starting process and all processes mentioned in the constraints.

The function returns different values based on the deadlock analysis:
- 0: Configuration is valid with no deadlocks
- -1: Configuration has hard deadlocks or is inconsistent  
- >0: Configuration has soft deadlocks (return value indicates number of soft edges)

For soft deadlocks, the function identifies one arbitrary soft cycle and returns a list of its soft edges for potential resolution. The function prioritizes checking constraint-involved processes before the starting process to handle the most constrained situations first.

## Parameters / Member Variables
- : Pointer to the PGPROC structure representing the starting process for deadlock checking

## Dependencies
- Functions called/Symbols referenced:
  - ExpandConstraints
  - FindLockCycle  
  - PGPROC (struct type)
  - EDGE (struct type)
  - curConstraints (global array)
  - possibleConstraints (global array)
  - nCurConstraints (global variable)
  - nPossibleConstraints (global variable)
  - maxPossibleConstraints (global variable)
- Called from (representative examples):
  - DeadLockCheckRecurse

## Notes and Other Information
- The function is static (internal to deadlock.c) and part of the deadlock detection algorithm
- Ensures sufficient space exists for FindLockCycle's output before proceeding
- Checks both waiter and blocker processes from each constraint to ensure comprehensive cycle detection
- The starting process is checked last to prioritize resolving constraint-related cycles first
- Soft edges from detected cycles are stored in possibleConstraints array for later use by the recursive algorithm
- Hard deadlocks indicate situations that cannot be resolved through queue rearrangement and require transaction abortion
- The function's return value drives the decision-making in DeadLockCheckRecurse about whether to continue searching or abort