# BgBufferSync

## Location
[src/backend/storage/buffer/bufmgr.c:3177-3474](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L3177-L3474)

## Overview
BgBufferSync periodically writes out dirty buffers in the background, implementing PostgreSQL's LRU-based buffer cleaning strategy with sophisticated allocation rate tracking and adaptive scanning.

## Definition
```c
bool BgBufferSync(WritebackContext *wb_context)
```

## Detailed Description
BgBufferSync is called periodically by the background writer process to proactively clean dirty buffers before they are needed for new allocations. It implements a sophisticated algorithm that tracks buffer allocation rates, maintains moving averages of allocation patterns, and estimates the density of reusable buffers. The function operates by scanning ahead of the strategy point (freelist clock sweep), cleaning dirty buffers to stay ahead of allocation demand. It uses adaptive algorithms to balance between being too aggressive (wasting I/O) and too conservative (causing allocation delays). The function can enter hibernation mode when there's little allocation activity and the strategy sweep has been "lapped".

## Parameters / Member Variables
- `wb_context`: Writeback context for batching and managing I/O operations
- Returns: `bool` indicating whether the bgwriter should hibernate (true if strategy clock was lapped and no recent allocations)

## Dependencies
- Functions called/Symbols referenced:
  - [StrategySyncStart](../S/StrategySyncStart.md)
  - [SyncOneBuffer](../S/SyncOneBuffer.md)
  - BUF_WRITTEN
  - BUF_REUSABLE
- Called from (representative examples):
  - [BackgroundWriterMain](BackgroundWriterMain.md)
  - RelationGetNumberOfBlocks

## Notes and Other Information
- Implements LRU-based buffer cleaning strategy that stays ahead of allocation demand
- Uses sophisticated moving averages to track allocation rates and buffer density
- Can be disabled by setting bgwriter_lru_maxpages to 0
- Includes adaptive algorithms to handle varying workload patterns
- Supports hibernation mode when there's minimal allocation activity
- Tracks multiple statistics including smoothed allocation rates and buffer density estimates
- Balances between staying ahead of allocations and avoiding excessive I/O
- Uses configurable parameters like bgwriter_lru_multiplier for tuning aggressiveness

## Simplified Source

```c
// Simplified version of BgBufferSync
bool BgBufferSync(WritebackContext *wb_context) {
    // Static variables to track state between calls
    static bool saved_info_valid = false;
    static int prev_strategy_buf_id, next_to_clean;
    static uint32 prev_strategy_passes, next_passes;
    static float smoothed_alloc = 0;
    static float smoothed_density = 10.0;

    // Get current strategy point and recent allocation count
    int strategy_buf_id;
    uint32 strategy_passes, recent_alloc;
    strategy_buf_id = StrategySyncStart(&strategy_passes, &recent_alloc);

    // Update buffer allocation statistics
    PendingBgWriterStats.buf_alloc += recent_alloc;

    // Early exit if LRU scanning is disabled
    if (bgwriter_lru_maxpages <= 0) {
        saved_info_valid = false;
        return true;  // OK to hibernate
    }

    // Calculate how many buffers to scan before catching up with strategy point
    int bufs_to_lap;
    if (saved_info_valid) {
        // Compute distance traveled by strategy point since last call
        long strategy_delta = strategy_buf_id - prev_strategy_buf_id;
        strategy_delta += (strategy_passes - prev_strategy_passes) * NBuffers;

        // Determine our position relative to strategy point
        if (we_are_ahead_of_strategy_point) {
            bufs_to_lap = calculate_buffers_until_we_lap_strategy();
        } else {
            // We're behind - jump to strategy point and start there
            next_to_clean = strategy_buf_id;
            next_passes = strategy_passes;
            bufs_to_lap = NBuffers;
        }
    } else {
        // First time or after being disabled - start at strategy point
        next_to_clean = strategy_buf_id;
        next_passes = strategy_passes;
        bufs_to_lap = NBuffers;
    }

    // Update tracking variables for next call
    prev_strategy_buf_id = strategy_buf_id;
    prev_strategy_passes = strategy_passes;
    saved_info_valid = true;

    // Update moving averages of allocation rate and buffer density
    if (strategy_delta > 0 && recent_alloc > 0) {
        float scans_per_alloc = (float)strategy_delta / recent_alloc;
        smoothed_density = update_moving_average(smoothed_density, scans_per_alloc);
    }

    smoothed_alloc = update_allocation_average(smoothed_alloc, recent_alloc);

    // Estimate how many buffers we need to clean
    int upcoming_alloc_est = (int)(smoothed_alloc * bgwriter_lru_multiplier);
    int reusable_buffers_est = estimate_reusable_buffers_ahead();

    // Ensure minimum scanning even during low activity
    int min_scan_buffers = calculate_minimum_scan_amount();
    if (upcoming_alloc_est < min_scan_buffers + reusable_buffers_est) {
        upcoming_alloc_est = min_scan_buffers + reusable_buffers_est;
    }

    // Main scanning loop: clean dirty buffers until we have enough
    int num_to_scan = bufs_to_lap;
    int num_written = 0;
    int reusable_buffers = reusable_buffers_est;

    while (num_to_scan > 0 && reusable_buffers < upcoming_alloc_est) {
        // Try to sync one buffer
        int sync_state = SyncOneBuffer(next_to_clean, true, wb_context);

        // Advance to next buffer (circular)
        if (++next_to_clean >= NBuffers) {
            next_to_clean = 0;
            next_passes++;
        }
        num_to_scan--;

        // Track results
        if (sync_state & BUF_WRITTEN) {
            reusable_buffers++;
            if (++num_written >= bgwriter_lru_maxpages) {
                PendingBgWriterStats.maxwritten_clean++;
                break;  // Hit write limit
            }
        } else if (sync_state & BUF_REUSABLE) {
            reusable_buffers++;
        }
    }

    // Update statistics
    PendingBgWriterStats.buf_written_clean += num_written;

    // Update density estimate based on our scan results
    update_density_estimate_from_scan_results();

    // Return true if OK to hibernate (strategy lapped and no recent allocs)
    return (bufs_to_lap == 0 && recent_alloc == 0);
}
```

Key simplifications made:
- Removed detailed debug logging and conditional compilation blocks
- Consolidated complex position tracking logic into simplified conditionals
- Abstracted moving average calculations into conceptual function calls
- Simplified buffer position arithmetic and wrap-around handling
- Focused on the main algorithm flow while preserving core logic
- Removed platform-specific considerations and detailed error handling
- Consolidated similar statistical tracking operations