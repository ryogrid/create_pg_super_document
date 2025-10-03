# subcoloronerow

## Location
[src/backend/regex/regc_color.c:885-915](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_color.c#L885-L915)

## Overview
The  function processes one row of the high colormap during regular expression compilation, applying subcolor processing and creating NFA arcs for each entry in the row.

## Definition

```c
static void
subcoloronerow(struct vars *v,
			   int rownum,
			   struct state *lp,
			   struct state *rp,
			   color *lastsubcolor)
```
## Detailed Description
This function is a helper for  that handles the processing of a single row in the high colormap. It iterates through each entry in the specified row, applies the  function to determine the appropriate subcolor, and creates NFA arcs when the subcolor changes. This optimization avoids creating redundant arcs for consecutive entries that map to the same subcolor.

The function operates on the high colormap structure, which is used for efficient color mapping in regular expression compilation. It ensures that state transitions are properly created in the NFA (Nondeterministic Finite Automaton) for each distinct subcolor encountered.

## Parameters / Member Variables
- `*v`: Pointer to the vars structure containing compilation context and NFA information
- `rownum`: The row number in the high colormap to process
- `*lp`: Left state pointer for creating NFA arcs
- `*rp`: Right state pointer for creating NFA arcs
- `*lastsubcolor`: Pointer to track the last subcolor processed to avoid duplicate arcs
## Dependencies
- Functions called/Symbols referenced:
  - : Determines the subcolor for a given color entry
  - : Creates a new arc in the NFA between states
  - : Error checking macro
  - : Arc type constant
- Called from (representative examples):
  - : Processes single character subcoloring
  - : Processes character range subcoloring

## Notes and Other Information
- This is a static helper function used internally within the regex color processing module
- The function uses error checking () after critical operations to ensure compilation integrity
- Performance optimization: only creates new arcs when the subcolor actually changes
- Part of the PostgreSQL regular expression engine's color mapping system for efficient pattern matching

## Simplified Source

```c
static void
subcoloronerow(struct vars *v, int rownum, struct state *lp, struct state *rp, color *lastsubcolor) {
    struct colormap *cm = v->cm;
    color *pco;
    int i;

    // Get pointer to start of this row in high colormap
    pco = &cm->hicolormap[rownum * cm->hiarraycols];

    // Process each entry in the row
    for (i = 0; i < cm->hiarraycols; pco++, i++) {
        // Get subcolor for this entry
        color sco = subcolorhi(cm, pco);

        // Create arc only if subcolor changed (optimization)
        if (sco != *lastsubcolor) {
            newarc(v->nfa, PLAIN, sco, lp, rp);
            *lastsubcolor = sco;
        }
    }
}
```