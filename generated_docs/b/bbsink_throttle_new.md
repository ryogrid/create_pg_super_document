# bbsink_throttle_new

## Location
[src/backend/backup/basebackup_throttle.c:68-95](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_throttle.c#L68-L95)

## Overview
Creates a new basebackup sink that performs bandwidth throttling and forwards data to a successor sink in the basebackup pipeline.

## Definition


## Detailed Description
The  function creates and initializes a throttling basebackup sink that controls the rate of data transfer during base backups. It allocates and configures a  structure with throttling parameters based on the specified maximum transfer rate. The sink operates by calculating a throttling sample size and minimum time unit to regulate data flow, ensuring backup operations don't overwhelm system resources or network bandwidth.

The function sets up the throttling mechanism by:
- Calculating the throttling sample size based on the maximum rate and throttling frequency
- Computing the minimum time unit for transferring the sample data
- Establishing the sink operation callbacks through 

## Parameters / Member Variables
- : Pointer to the next sink in the basebackup pipeline chain that will receive forwarded data
- : Maximum transfer rate in kilobytes per second for the throttling mechanism

## Dependencies
- Functions called/Symbols referenced:
  -  (memory allocation)
  -  (structure type)
  -  (operation callbacks)
  -  (constant for throttling calculations)
  -  (time conversion constant)
- Called from (representative examples):
  -  (src/backend/backup/basebackup.c:1038)
  -  (src/include/backup/basebackup_sink.h:292)

## Notes and Other Information
- The function uses assertions to ensure valid parameters (non-null next sink and positive maxrate)
- Throttling calculations convert the rate from KB/s to bytes per throttling interval
- The throttling mechanism operates at a frequency defined by 
- Memory is allocated using  to ensure zero-initialization of the structure
- Part of PostgreSQL's basebackup throttling subsystem for controlling backup transfer rates