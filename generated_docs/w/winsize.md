# winsize

## Location
[src/interfaces/libpq/fe-print.c:100-329](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-print.c#L100-L329)

## Overview
A local struct definition that provides a fallback implementation for terminal window size information when the system's TIOCGWINSZ ioctl support is not available.

## Definition


## Detailed Description
The winsize struct is conditionally defined in the PQprint function within fe-print.c as a fallback mechanism for systems that don't have the standard TIOCGWINSZ ioctl capability. When TIOCGWINSZ is available, the system's native struct winsize is used instead. This struct holds terminal dimensions that are used to determine whether query results should be piped to a pager program based on the estimated output size relative to the terminal screen.

The struct is used within the PQprint function to make intelligent decisions about output formatting - specifically whether the output will exceed the terminal's display capacity and should therefore be piped through a pager for better user experience.

## Parameters / Member Variables
- : Number of rows (lines) available in the terminal window
- : Number of columns (characters) available in the terminal window

## Dependencies
- Functions called/Symbols referenced:
  - ioctl (when TIOCGWINSZ is available)
  - TIOCGWINSZ (system constant for getting window size)
- Called from (representative examples):
  - PQprint (where this struct is defined and used)
- Referenced by:
  - [print_aligned_text](../p/print_aligned_text.md) (in src/fe_utils/print.c)
  - [print_aligned_vertical](../p/print_aligned_vertical.md) (in src/fe_utils/print.c)
  - [PageOutput](../P/PageOutput.md) (in src/fe_utils/print.c)

## Notes and Other Information
- This is a compatibility fallback - modern systems typically have TIOCGWINSZ support
- Default values of 24 rows and 80 columns are used when terminal size cannot be determined
- The struct is used to calculate whether query output should be automatically piped to a pager
- The PQprint function itself is considered legacy and may be removed in future versions as noted in the source comments
- This implementation is specific to the libpq client library's printing functionality