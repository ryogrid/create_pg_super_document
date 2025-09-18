# HistControl

## Location
[src/bin/psql/settings.h:71-72](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/settings.h#L71-L72)

## Overview
HistControl is an enumeration type that defines command history filtering behavior in psql, controlling which commands are saved to or excluded from the command history.

## Definition


## Detailed Description
The HistControl enumeration provides a set of flags that determine how psql manages its command history storage. It implements a bitmask-based system where individual flags can be combined to achieve different history filtering behaviors. This enumeration is designed to mirror similar functionality found in bash and other shell environments, giving users familiar control over which commands are preserved in their interactive session history.

The enumeration uses powers of 2 for its base values (except hctl_none), allowing them to be combined using bitwise OR operations. The hctl_ignoreboth value demonstrates this by being defined as the combination of the two primary filtering flags.

## Parameters / Member Variables
- : No history filtering; all commands are saved to history (value: 0)
- : Ignore commands that begin with a space character (value: 1)  
- : Ignore duplicate consecutive commands in history (value: 2)
- : Apply both ignorespace and ignoredups filtering (value: 3)

## Dependencies
- Functions called/Symbols referenced:
  - None (standalone enumeration)
- Called from (representative examples):
  - [_psqlSettings](../p/_psqlSettings.md) (used as histcontrol member field)

## Notes and Other Information
This enumeration is used within the _psqlSettings structure to control command history behavior in psql sessions. The design follows common Unix shell conventions where commands starting with spaces are considered "private" and duplicate commands clutter the history unnecessarily. Users can set the HISTCONTROL psql variable to control this behavior, with the variable value being converted to the appropriate HistControl enumeration value. The bitmask design allows for flexible combinations of filtering rules while maintaining efficiency in implementation.