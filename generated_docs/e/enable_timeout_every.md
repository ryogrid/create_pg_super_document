# enable_timeout_every

## Location
src/backend/utils/misc/timeout.c: 584 - 606

## Overview
Enables a timeout to fire periodically at regular intervals, with a specified delay between each firing.

## Definition

```c
void
enable_timeout_every(TimeoutId id, TimestampTz fin_time, int delay_ms)
```
## Detailed Description
This function configures a timeout to trigger repeatedly at regular intervals. Unlike one-time timeouts, this creates a periodic timer that continues to fire every delay_ms milliseconds starting from fin_time. The function temporarily disables alarm interrupts for thread safety during configuration, sets up the timeout using the internal enable_timeout mechanism, and then reschedules the system alarm to accommodate the new periodic timeout.

The periodic nature makes this function particularly useful for recurring operations like progress reporting, health checks, or periodic maintenance tasks that need to occur at regular intervals during long-running operations.

## Parameters / Member Variables
- `id`: TimeoutId identifying which timeout handler to enable
- `fin_time`: TimestampTz specifying when the timeout should first fire
- `delay_ms`: int specifying the delay in milliseconds between subsequent firings

## Dependencies
- Functions called/Symbols referenced:
  - disable_alarm
  - GetCurrentTimestamp
  - enable_timeout
  - schedule_alarm
- Called from (representative examples):
  - enable_startup_progress_timeout
  - DisableTimeoutParams

## Notes and Other Information
- The function temporarily disables alarm interrupts during configuration to ensure atomic setup
- After the first firing at fin_time, subsequent firings occur every delay_ms milliseconds
- The delay is specified in milliseconds for fine-grained control over timing
- Uses the PostgreSQL timestamp system for precise timing calculations
- Part of PostgreSQL's unified timeout management system introduced to handle various timing-sensitive operations