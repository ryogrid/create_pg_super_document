# subcolorcvec

## Location
[src/backend/regex/regc_color.c:522-623](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_color.c#L522-L623)

## Overview
Allocates new subcolors for all characters and ranges in a character vector (cvec) and creates corresponding NFA arcs between two states.

## Definition

```c
static void
subcolorcvec(struct vars *v,
			 struct cvec *cv,
			 struct state *lp,
			 struct state *rp)
```
## Detailed Description
The  function is a key component of PostgreSQL's regex compilation system. It processes a character vector (cvec) containing individual characters, character ranges, and character classes, converting them into subcolors and creating NFA arcs. The function optimizes performance by avoiding duplicate arc creation through the  state variable. It handles three types of character specifications: ordinary individual characters, character ranges, and character classes. For character classes, it dynamically expands the hicolormap if needed and processes all relevant color combinations.

## Parameters / Member Variables
- `*v`: Pointer to the regex compilation variables structure
- `*cv`: Pointer to the character vector containing characters, ranges, and character classes to process
- `*lp`: Pointer to the source state for the NFA arcs
- `*rp`: Pointer to the destination state for the NFA arcs
## Dependencies
- Functions called/Symbols referenced:
  - [subcoloronechr](subcoloronechr.md) (processes individual characters)
  - [subcoloronerange](subcoloronerange.md) (processes character ranges)
  - [subcolor](subcolor.md) (gets subcolor for simple characters)
  - [subcolorhi](subcolorhi.md) (gets subcolor for high colormap entries)
  - [newarc](../n/newarc.md) (creates NFA arcs)
  - [newhicolorcols](../n/newhicolorcols.md) (expands hicolormap columns)
  - NOERR (error checking macro)
- Called from (representative examples):
  - [charclass](../c/charclass.md) (at line 1507)
  - [charclasscomplement](../c/charclasscomplement.md) (at line 1536)
  - [brackpart](../b/brackpart.md) (at lines 1810, 1876)
  - [onechr](../o/onechr.md) (at line 1925)
  - [wordchrs](../w/wordchrs.md) (at line 2011)

## Notes and Other Information
- Does not return a value (void function)
- Uses optimization to avoid creating duplicate arcs by tracking the last subcolor created
- Handles both simple characters (≤ MAX_SIMPLE_CHR) and complex characters differently for efficiency
- Automatically expands the hicolormap when processing new character classes
- Part of the regex compilation process that converts high-level character specifications into low-level NFA transitions
- Uses bit manipulation for character class processing in the hicolormap

## Simplified Source

```c
static void subcolorcvec(struct vars *v,
                        struct cvec *cv,
                        struct state *lp,
                        struct state *rp)
{
    struct colormap *cm = v->cm;
    color lastsubcolor = COLORLESS;
    chr ch, from, to;
    const chr *p;
    int i;

    // Process ordinary characters
    for (p = cv->chrs, i = cv->nchrs; i > 0; p++, i--) {
        ch = *p;
        subcoloronechr(v, ch, lp, rp, &lastsubcolor);
        NOERR();
    }

    // Process character ranges
    for (p = cv->ranges, i = cv->nranges; i > 0; p += 2, i--) {
        from = *p;
        to = *(p + 1);

        if (from <= MAX_SIMPLE_CHR) {
            // Handle simple chars individually
            chr lim = (to <= MAX_SIMPLE_CHR) ? to : MAX_SIMPLE_CHR;

            while (from <= lim) {
                color sco = subcolor(cm, from);
                NOERR();

                // Create arc only if subcolor changed
                if (sco != lastsubcolor) {
                    newarc(v->nfa, PLAIN, sco, lp, rp);
                    NOERR();
                    lastsubcolor = sco;
                }
                from++;
            }
        }

        // Handle any remaining range above MAX_SIMPLE_CHR
        if (from < to)
            subcoloronerange(v, from, to, lp, rp, &lastsubcolor);
        else if (from == to)
            subcoloronechr(v, from, lp, rp, &lastsubcolor);
        NOERR();
    }

    // Process character class if present
    if (cv->cclasscode >= 0) {
        int classbit;
        color *pco;
        int r, c;

        // Expand hicolormap if needed for this character class
        if (cm->classbits[cv->cclasscode] == 0) {
            cm->classbits[cv->cclasscode] = cm->hiarraycols;
            newhicolorcols(cm);
            NOERR();
        }

        // Apply subcolorhi() and create arcs for relevant entries
        classbit = cm->classbits[cv->cclasscode];
        pco = cm->hicolormap;

        for (r = 0; r < cm->hiarrayrows; r++) {
            for (c = 0; c < cm->hiarraycols; c++) {
                if (c & classbit) {
                    color sco = subcolorhi(cm, pco);
                    NOERR();

                    // Create arc if subcolor changed
                    if (sco != lastsubcolor) {
                        newarc(v->nfa, PLAIN, sco, lp, rp);
                        NOERR();
                        lastsubcolor = sco;
                    }
                }
                pco++;
            }
        }
    }
}
```