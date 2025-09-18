# assign_checkpoint_completion_target

## Location
[src/backend/access/transam/xlog.c:2200-2206](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L2200-L2206)

## Overview
Assignment hook function that updates the checkpoint completion target configuration and recalculates checkpoint segment parameters.

## Definition
void assign_checkpoint_completion_target(double newval, void *extra)

## Detailed Description
This function serves as a PostgreSQL GUC (Grand Unified Configuration) assignment hook for the checkpoint_completion_target parameter. When the checkpoint_completion_target configuration is changed, this function is called to:

1. Update the global CheckPointCompletionTarget variable with the new value
2. Trigger recalculation of checkpoint segments to ensure the new completion target is properly reflected in checkpoint scheduling

The checkpoint_completion_target parameter controls the fraction of the checkpoint interval over which checkpoints should be spread. A value of 0.5 means checkpoints should complete halfway through the checkpoint cycle, while 0.9 means they should complete 90% of the way through the cycle. This affects both I/O distribution and WAL space calculations.

## Parameters / Member Variables
- `newval`: The new value for checkpoint_completion_target (typically between 0.0 and 1.0)
- `extra`: Additional context data (unused in this implementation, as per GUC hook convention)

## Dependencies
- Functions called/Symbols referenced:
  - [CalculateCheckpointSegments](../C/CalculateCheckpointSegments.md)
- Called from:
  - GUC system (via GUC_HOOKS_H)

## Notes and Other Information
- This function is part of PostgreSQL's GUC (Grand Unified Configuration) hook system
- Changes take effect immediately without requiring server restart
- The completion target affects how checkpoint I/O is spread over time and influences WAL space calculations
- Essential for dynamic tuning of checkpoint performance characteristics
- Works in conjunction with assign_max_wal_size for complete checkpoint behavior configuration
- The completion target must be between 0.0 and 1.0, with typical values ranging from 0.5 to 0.9