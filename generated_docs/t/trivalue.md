# trivalue

## Location
src/bin/pg_dump/pg_backup.h: 37 - 38

## Overview
A three-state enumeration type used throughout PostgreSQL client utilities to represent boolean-like values with a default/unset state.

## Definition


## Detailed Description
The  enum provides a three-state boolean representation commonly needed in PostgreSQL client applications where a setting can be explicitly enabled, explicitly disabled, or left to use a default value. This is particularly useful for command-line utilities and connection parameters where users may want to override defaults or leave them unspecified.

## Parameters / Member Variables
- : Represents the default/unset state - no explicit choice has been made
- : Represents an explicitly disabled/false state
- : Represents an explicitly enabled/true state

## Dependencies
- Functions called/Symbols referenced:
  - None (primitive enum type)
- Called from (representative examples):
  -  struct in pg_backup.h and fe_utils/connect_utils.h
  - Various main functions in client utilities (pg_amcheck, pg_dumpall, psql, etc.)
  - Connection-related functions like  and 
  - psql settings and command handling

## Notes and Other Information
This enum is widely used across PostgreSQL client utilities for handling three-state logic, particularly in connection parameters and command-line option processing. It allows utilities to distinguish between a user explicitly setting a boolean option versus leaving it unspecified to use system defaults.