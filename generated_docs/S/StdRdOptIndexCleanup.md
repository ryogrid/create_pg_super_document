# StdRdOptIndexCleanup

## Location
src/include/utils/rel.h: 334 - 335

## Overview
An enumeration that defines the possible values for controlling index cleanup behavior during VACUUM operations on table relations.

## Definition


## Detailed Description
This enum is used as part of the StdRdOptions structure to control index vacuuming and cleanup behavior for table relations. It provides three distinct modes for index cleanup during VACUUM operations:

- **AUTO**: The default behavior where PostgreSQL automatically determines whether index cleanup should be performed based on system conditions
- **OFF**: Explicitly disables index cleanup during vacuum operations  
- **ON**: Forces index cleanup to be performed during vacuum operations

The enum is used in conjunction with the vacuum_index_cleanup table storage parameter and is evaluated during vacuum operations to determine the appropriate index cleanup strategy.

## Parameters / Member Variables
- : Default value (0) that enables automatic determination of index cleanup necessity
- : Disables index cleanup during vacuum operations
- : Forces index cleanup to be performed during vacuum operations

## Dependencies
- Functions called/Symbols referenced:
  - Used as part of StdRdOptions structure
- Called from (representative examples):
  - vacuum_rel (src/backend/commands/vacuum.c:2154)
  - StdRdOptions structure (src/include/utils/rel.h:344)

## Notes and Other Information
- This enum is defined in src/include/utils/rel.h:329-334
- The vacuum_index_cleanup table option corresponds to this enum and can be set via CREATE TABLE or ALTER TABLE statements
- The enum values are mapped to VACOPTVALUE constants during vacuum processing in vacuum_rel()
- This feature provides fine-grained control over index maintenance during vacuum operations, which can be useful for performance tuning in specific scenarios
- The AUTO setting allows PostgreSQL to make intelligent decisions about when index cleanup is beneficial versus when it should be skipped