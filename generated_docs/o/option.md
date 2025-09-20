# option

## Location
[src/include/getopt_long.h:16-23](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/getopt_long.h#L16-L23)

## Overview
The  struct defines the structure for command-line option specifications used by the  function for parsing long command-line options in PostgreSQL utilities and applications.

## Definition

```c
struct option
{
	const char *name;
	int			has_arg;
	int		   *flag;
	int			val;
};
```
## Detailed Description
The  struct is part of PostgreSQL's implementation of the GNU-style long option parsing functionality. It is defined in  and is used extensively throughout PostgreSQL's command-line utilities for defining long command-line options (options that start with ). Each  struct represents a single long option that can be parsed by the  function.

This struct is conditionally defined only when the system doesn't already provide  (when  is not defined), ensuring compatibility across different platforms. The struct works in conjunction with the  function to provide a standardized way of handling command-line arguments across all PostgreSQL utilities.

## Parameters / Member Variables
- : A pointer to the long option name (without the leading ). For example, "help", "version", "port", etc.
- : Specifies whether the option takes an argument. Valid values are  (0),  (1), or  (2)
- : If NULL,  returns the value in . If non-NULL,  sets the variable pointed to by  to the value in  and returns 0
- : The value to return or store when this option is encountered. Typically set to a character code for short options or a unique integer for long-only options

## Dependencies
- Functions called/Symbols referenced:
  - [flag](../f/flag.md) (member variable reference)
- Called from (representative examples):
  - getopt_long (primary function that uses this struct)
  - Multiple PostgreSQL utilities including:
    - initdb
    - pg_dump
    - pg_basebackup
    - psql
    - pg_upgrade
    - Various other command-line tools

## Notes and Other Information
- This struct is part of PostgreSQL's portability layer, ensuring consistent long option parsing across platforms
- The struct is used in conjunction with predefined constants: , , and 
- Arrays of  structs are typically terminated with a null entry (all fields set to 0/NULL)
- This implementation provides GNU getopt_long compatibility for systems that don't natively support it
- The struct is heavily used throughout PostgreSQL's command-line utilities, with over 200 references across the codebase
- Located in 