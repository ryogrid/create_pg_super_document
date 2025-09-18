# IOObject

## Location
src/include/pgstat.h: 282 - 283

## Overview
IOObject is an enumeration that categorizes different types of database objects for I/O statistics tracking in PostgreSQL's buffer management system.

## Definition


## Detailed Description
IOObject defines the types of database objects that can be tracked for I/O operations in PostgreSQL's statistics collection system. This enumeration helps categorize I/O activities based on whether they involve regular persistent relations or temporary relations, allowing for more granular performance monitoring and analysis of different workload patterns.

## Parameters / Member Variables
- : Represents regular persistent database relations (tables, indexes, etc.) that are stored permanently on disk
- : Represents temporary relations that are created during query execution and typically have different I/O characteristics

## Dependencies
- Functions called/Symbols referenced:
  - Used in conjunction with IOContext and IOOp for comprehensive I/O tracking
- Called from (representative examples):
  - pgstat_count_io_op
  - pgstat_count_io_op_n
  - pgstat_count_io_op_time
  - pgstat_get_io_object_name
  - pgstat_tracks_io_object
  - Buffer management functions in bufmgr.c

## Notes and Other Information
- Used with the helper macro IOOBJECT_NUM_TYPES to determine the total number of object types
- Part of a comprehensive I/O statistics framework that includes IOContext and IOOp enumerations
- Enables differentiation between persistent and temporary relation I/O patterns for performance analysis
- Integrated into PostgreSQL's buffer management system for tracking read/write operations on different object types
- Essential for understanding database workload characteristics and optimizing I/O performance