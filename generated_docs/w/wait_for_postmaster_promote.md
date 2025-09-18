# wait_for_postmaster_promote

## Location
[src/bin/pg_ctl/pg_ctl.c:746-773](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L746-L773)

## Overview
Waits for a PostgreSQL standby server to complete promotion to become a primary server, monitoring the database state transition.

## Definition


## Detailed Description
The  function monitors the promotion process of a PostgreSQL standby server to primary status. It combines process liveness monitoring with database state checking to determine when promotion has completed successfully.

**Key behaviors:**
- Polls up to  times with sleep intervals between checks
- Monitors process liveness to detect premature termination during promotion
- Checks the database control file state using 
- Returns  when the database state reaches  (successful promotion)
- Returns  for process death, missing PID file, or timeout
- Provides progress feedback by printing dots during the wait

**Promotion monitoring logic:**
1. Verifies the postmaster process is still running
2. Checks if the PID file still exists  
3. Reads the database state from the control file
4. Considers promotion successful when state becomes 

This approach ensures that the function waits for the actual database state change rather than just the completion of the promote command.

## Parameters / Member Variables
This function takes no parameters but relies on global variables:
- : Maximum time to wait for promotion to complete
- : Path to the PostgreSQL PID file (used by )

## Dependencies
- Functions called/Symbols referenced:
  -  - Reads the PID from postmaster.pid file
  -  - Tests if process is still alive (with signal 0)
  -  - Reads database state from pg_control file
  -  - Prints progress dots
  -  - Cross-platform sleep function
- Called from (representative examples):
  -  in pg_ctl.c

## Notes and Other Information
- Used specifically for standby-to-primary promotion operations via 
- The  state indicates the server has successfully transitioned from standby to primary
- Process liveness checking prevents indefinite waiting if the postmaster crashes during promotion
- Timeout handling ensures pg_ctl doesn't hang on failed or slow promotions
- Progress indication helps users understand that promotion monitoring is active
- This function is part of PostgreSQL's streaming replication and high availability features