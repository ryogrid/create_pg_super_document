# UpdateCheckPointDistanceEstimate

## Location
src/backend/access/transam/xlog.c: 6763 - 6800

## Overview
UpdateCheckPointDistanceEstimate maintains a moving average estimate of WAL bytes generated between checkpoints, used to optimize WAL segment preallocation for performance.

## Definition
```c
static void UpdateCheckPointDistanceEstimate(uint64 nbytes)
```

## Detailed Description
This static function updates the estimate of WAL distance between checkpoints using an asymmetric moving average algorithm designed to handle bursty workloads. The algorithm immediately increases the estimate when the actual distance exceeds the current estimate, but slowly decreases it (using 90% of previous estimate plus 10% of current) when actual usage is lower. This approach ensures adequate preallocation for peak loads while gradually adapting to sustained changes in workload. The estimate is used by XLOGfileslop() to determine how many WAL segments to keep preallocated.

## Parameters / Member Variables
- `nbytes`: The number of bytes of WAL generated since the previous checkpoint

## Dependencies
- Functions called/Symbols referenced:
  - PrevCheckPointDistance (global variable)
  - CheckPointDistanceEstimate (global variable)
- Called from (representative examples):
  - CreateCheckPoint (in xlog.c:7295)
  - CreateRestartPoint (in xlog.c:7762)

## Notes and Other Information
- Static function only called internally within xlog.c
- Uses asymmetric moving average: quick increase (immediate), slow decrease (90%/10% blend)
- Designed to handle bursty workloads by catering to peak load requirements
- Ignores checkpoint cause - treats manual, backup-triggered, and automatic checkpoints equally
- Should converge to CheckpointSegments * wal_segment_size for max_wal_size triggered checkpoints
- Essential for WAL segment preallocation optimization in XLOGfileslop()
- Updates both PrevCheckPointDistance (exact) and CheckPointDistanceEstimate (smoothed)