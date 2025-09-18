# ds_state

## Location
src/backend/commands/tsearchcmds.c: 1641 - 1833

## Overview
ds_state is an enumerated type (enum) that defines the different parsing states used in the deserialize_deflist function for parsing parameter lists in text search configuration.

## Definition


## Detailed Description
The ds_state enum implements a finite state machine for parsing parameter lists from serialized text format back into PostgreSQL's DefElem structures. This is used primarily for text search dictionary and parser headline options. The parsing logic handles various quoting styles including unquoted strings, single-quoted strings, double-quoted strings, and escaped characters for backward compatibility.

## Parameters / Member Variables
- : Waiting for the start of a parameter key name
- : Currently reading an unquoted parameter key
- : Currently reading a quoted parameter key (within double quotes)
- : Waiting for the '=' separator between key and value
- : Waiting for the start of a parameter value
- : Currently reading a single-quoted parameter value
- : Currently reading a double-quoted parameter value
- : Currently reading an unquoted parameter value

## Dependencies
- Functions called/Symbols referenced:
  - [buildDefItem](../b/buildDefItem.md)
  - text_to_cstring
  - [DefElem](../D/DefElem.md)
- Called from (representative examples):
  - deserialize_deflist (local variable usage)

## Notes and Other Information
This enum is defined locally within the deserialize_deflist function in src/backend/commands/tsearchcmds.c:1631-1641. The state machine handles escape sequences, quote doubling for literal quotes, and comma/whitespace separation between parameters. The parsing supports both legacy unquoted formats and modern quoted formats for backward compatibility with older PostgreSQL versions.