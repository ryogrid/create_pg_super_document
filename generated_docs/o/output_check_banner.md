# output_check_banner

## Location
[src/bin/pg_upgrade/check.c:559-576](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L559-L576)

## Overview
Displays a formatted banner message indicating the start of consistency checks during PostgreSQL cluster upgrades, with different messages for live server checks versus offline checks.

## Definition

```c
void
output_check_banner(bool live_check)
```
## Detailed Description
This function outputs a standardized banner message to inform users that consistency checks are beginning during the pg_upgrade process. It provides two different banner formats: one for checks performed on a live running server and another for standard offline checks. The function uses the user_opts.check flag to determine if checks are enabled and the live_check parameter to distinguish between live and offline check scenarios.

The banner serves as a visual separator and progress indicator in the upgrade process, helping users understand what phase of the upgrade is currently executing.

## Parameters / Member Variables
- : Boolean flag indicating whether the checks are being performed on a live running server (true) or offline (false)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_log](../p/pg_log.md)
  - PG_REPORT
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- Uses user_opts.check global variable to determine if check mode is enabled
- Displays "Old Live Server" banner when both user_opts.check and live_check are true
- Displays standard "Consistency Checks" banner for all other scenarios
- Banner includes decorative dashes for visual formatting
- Part of the pg_upgrade user interface feedback system
- Function has external linkage (not static) and can be called from other compilation units