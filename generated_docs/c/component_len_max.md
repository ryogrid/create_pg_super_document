# component_len_max

## Location
[src/timezone/zic.c:864-902](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L864-L902)

## Overview
An enumerated constant that defines the maximum recommended length for a single component (directory or file name part) within a timezone file path in the PostgreSQL timezone compiler.

## Definition


## Detailed Description
The component_len_max constant is defined within the componentcheck function and specifies the maximum recommended length for individual path components in timezone file names. When a component exceeds this length, the timezone compiler issues a warning but continues processing. This limit is based on historical filesystem compatibility considerations and helps ensure that generated timezone files will be portable across different systems, particularly older ones with more restrictive filename length limits.

The value of 14 characters reflects traditional limitations of some older filesystems and maintains compatibility with systems that may have shorter filename component limits than modern filesystems.

## Parameters / Member Variables
- Value: 14 (constant integer representing maximum component length in characters)

## Dependencies
- Functions called/Symbols referenced:
  - [warning](../w/warning.md) (used to issue warnings when component length exceeds this limit)
- Called from (representative examples):
  - Used within componentcheck function for length validation

## Notes and Other Information
- Defined as a local enum within the componentcheck function scope
- Triggers warnings (not errors) when exceeded, allowing compilation to continue
- Based on historical filesystem compatibility requirements
- Part of the timezone file naming validation system in PostgreSQL's zic utility
- Helps ensure generated timezone files are portable across different operating systems and filesystem types