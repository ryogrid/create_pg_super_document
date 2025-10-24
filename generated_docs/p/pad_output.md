# pad_output

## Location
[src/tools/pg_bsd_indent/io.c:468-516](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/io.c#L468-L516)

## Overview
Writes tabs and spaces to move the current column position up to a desired target position in the output stream.

## Definition

```c
static int
pad_output(int current, int target)
			        /* writes tabs and blanks (if necessary) to
				 * get the current output position up to the
				 * target column */
    /* current: the current column value */
    /* target: position we want it at */
```
## Detailed Description
The  function is a static utility function in the PostgreSQL BSD indent tool that handles output formatting by inserting the appropriate combination of tabs and spaces to move from the current column position to a target column position. This is essential for proper code indentation and alignment.

The function implements an intelligent padding algorithm that:
- Uses tabs when  is enabled and when tab usage is efficient
- Applies PostgreSQL-specific tab rules when  is enabled
- Falls back to spaces for fine-grained positioning
- Optimizes output by using the most efficient combination of tabs and spaces

The PostgreSQL tab rules add special logic to prevent using tabs in certain scenarios where spaces provide better alignment consistency.

## Parameters / Member Variables
- `current`: The current column position (integer)
- `target`: The desired target column position (integer)
## Dependencies
- Functions called/Symbols referenced:
  - : Standard C library function for character output
- Called from (representative examples):
  - : Main line output function (multiple locations for different formatting contexts)

## Notes and Other Information
- Originally coded in November 1976 by D A Willcox of CAC
- Returns the new column position (target value if padding occurred, current value if no action needed)
- If current position is already at or beyond target, no action is taken
- The function considers tab size settings and PostgreSQL-specific formatting rules
- Uses  global variable to calculate optimal tab placement
- The  flag provides additional control over when tabs vs spaces are used
- Essential for maintaining consistent indentation and alignment in formatted code output

## Simplified Source

```c
static int pad_output(int current, int target) {
    int curr;

    // No padding needed if already at or past target
    if (current >= target)
        return current;

    curr = current;

    // Use tabs when enabled and efficient
    if (use_tabs) {
        int tcur;
        while ((tcur = tabsize * (1 + (curr - 1) / tabsize) + 1) <= target) {
            // Apply PostgreSQL tab rules if enabled
            char tab_char = (!postgres_tab_rules ||
                           tcur != curr + 1 ||
                           target >= tcur + tabsize) ? '\t' : ' ';
            putc(tab_char, output);
            curr = tcur;
        }
    }

    // Fill remaining space with spaces
    while (curr++ < target)
        putc(' ', output);

    return target;
}
```