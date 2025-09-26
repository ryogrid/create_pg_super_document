# CommitTsShmemBuffers

## Location
src/backend/access/transam/commit_ts.c: 506 - 518

## Overview
Determines the number of shared memory buffers to allocate for the CommitTS (commit timestamp) SLRU, either by auto-tuning based on shared buffers or using a configured value within allowed limits.

## Definition
```c
static int CommitTsShmemBuffers(void)
```

## Detailed Description
This function calculates the appropriate number of buffers for the CommitTS SLRU (Simple Least Recently Used) cache. The CommitTS system stores commit timestamps for transactions, and this function ensures optimal buffer allocation for performance.

The function implements two allocation strategies:
1. **Auto-tuning mode**: When `commit_timestamp_buffers` is 0, it uses `SimpleLruAutotuneBuffers(512, 1024)` to automatically determine buffer count based on shared memory configuration (2MB for every 1GB of shared buffers, up to 8MB)
2. **Manual configuration mode**: When a specific value is configured, it enforces bounds between 16 (minimum) and `SLRU_MAX_ALLOWED_BUFFERS` (maximum)

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - SimpleLruAutotuneBuffers
  - SLRU_MAX_ALLOWED_BUFFERS
- Called from (representative examples):
  - CommitTsShmemSize
  - CommitTsShmemInit

## Notes and Other Information
- The function is static, indicating it's only used within the commit_ts.c module
- The auto-tuning logic helps optimize memory usage based on the system's shared buffer configuration
- The minimum buffer count of 16 ensures basic functionality even with small configurations
- This is part of PostgreSQL's commit timestamp tracking infrastructure, which records when transactions commit