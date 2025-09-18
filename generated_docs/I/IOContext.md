# IOContext

## Location
src/include/pgstat.h: 292 - 293

## Overview
IOContext is an enumeration that defines different operational contexts for I/O operations, helping PostgreSQL categorize and track I/O statistics based on the type of database activity being performed.

## Definition


## Detailed Description
IOContext categorizes I/O operations based on the database activity context in which they occur. This enumeration enables PostgreSQL to track and analyze I/O patterns for different types of operations, allowing for better understanding of performance characteristics and optimization opportunities. Different contexts may have distinct I/O patterns and resource requirements, making this categorization valuable for performance monitoring and tuning.

## Parameters / Member Variables
- : I/O operations during bulk read activities such as large table scans or data loading operations
- : I/O operations during bulk write activities such as large INSERT, COPY, or CREATE TABLE AS SELECT operations
- : Regular I/O operations during normal query processing, including typical SELECT, INSERT, UPDATE, and DELETE operations
- : I/O operations performed during vacuum processes, including both manual and automatic vacuum operations

## Dependencies
- Functions called/Symbols referenced:
  - Used in conjunction with IOObject and IOOp for comprehensive I/O tracking
- Called from (representative examples):
  - pgstat_count_io_op
  - pgstat_count_io_op_n
  - pgstat_count_io_op_time
  - pgstat_get_io_context_name
  - pgstat_tracks_io_object
  - Buffer management functions throughout bufmgr.c
  - Resource management functions

## Notes and Other Information
- Used with the helper macro IOCONTEXT_NUM_TYPES to determine the total number of context types
- Essential component of PostgreSQL's comprehensive I/O statistics framework
- Enables performance analysis by operation type, helping identify bottlenecks in specific workload patterns
- Integrated deeply into the buffer management system for real-time I/O tracking
- Different contexts may trigger different buffer management strategies and caching behaviors
- Useful for database administrators to understand which types of operations are consuming I/O resources