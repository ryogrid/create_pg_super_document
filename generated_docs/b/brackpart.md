# brackpart

## Location
[src/backend/regex/regcomp.c:1763-1885](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regcomp.c#L1763-L1885)

## Overview
Handles one item or range within a bracket expression, parsing various bracket element types and creating appropriate NFA arcs.

## Definition
```c
static void brackpart(struct vars *v, struct state *lp, struct state *rp, bool *have_cclassc)
```

## Detailed Description
The `brackpart` function processes individual components within bracket expressions, handling various element types including plain characters, ranges, collating elements, equivalence classes, character classes, and complemented character classes. It uses a switch statement to handle different token types (PLAIN, RANGE, COLLEL, ECLASS, CCLASS, CCLASSS, CCLASSC). For ranges, it processes both start and end characters and creates a range using the `range` function. The function includes special handling for complemented character classes by marking them in the `have_cclassc` array for deferred processing. It also includes portability warnings for character ranges since they may not be portable across different character encodings.

## Parameters / Member Variables
- `v`: Pointer to the vars structure containing compilation state and current parsing position
- `lp`: Pointer to the left/start state for the bracket part
- `rp`: Pointer to the right/end state for the bracket part  
- `have_cclassc`: Boolean array tracking which complemented character classes were encountered

## Dependencies
- Functions called/Symbols referenced:
  - ERR (error reporting macro)
  - NEXT (advances to next token)
  - SEE (checks current token type)
  - [onechr](../o/onechr.md) (handles single character)
  - [scanplain](../s/scanplain.md) (scans plain text within delimiters)
  - INSIST (assertion with error handling)
  - [element](../e/element.md) (processes collating elements)
  - [eclass](../e/eclass.md) (handles equivalence classes)
  - [subcolorcvec](../s/subcolorcvec.md) (creates arcs for character vector)
  - lookupcclass (looks up character class)
  - [charclass](../c/charclass.md) (handles character classes)
  - [range](../r/range.md) (creates character ranges)
  - NOERR/NOTE (error handling macros)
  - Various constants: REG_ERANGE, REG_ECOLLATE, REG_ECTYPE, REG_ASSERT, REG_ICASE, REG_UUNPORT
- Called from:
  - [bracket](bracket.md) (main bracket expression handler)

## Notes and Other Information
- Supports multiple bracket element types: plain chars, ranges, collating elements, equivalence classes, character classes
- Defers processing of complemented character classes to avoid color bookkeeping issues
- Includes portability warnings for character ranges (REG_UUNPORT)
- Handles both case-sensitive and case-insensitive matching via REG_ICASE flag
- Contains extensive error checking for malformed bracket expressions
- Located in src/backend/regex/regcomp.c:1763-1885

## Simplified Source

```c
static void brackpart(struct vars *v, struct state *lp, struct state *rp, bool *have_cclassc) {
    chr startc, endc;
    struct cvec *cv;
    enum char_classes cls;
    const chr *startp, *endp;

    // Parse different bracket element types
    switch (v->nexttype) {
        case RANGE:
            ERR(REG_ERANGE);
            return;
        case PLAIN:
            startc = v->nextvalue;
            NEXT();
            // Handle single character (not a range)
            if (!SEE(RANGE)) {
                onechr(v, startc, lp, rp);
                return;
            }
            break;
        case COLLEL:
            startp = v->now;
            endp = scanplain(v);
            INSIST(startp < endp, REG_ECOLLATE);
            startc = element(v, startp, endp);
            break;
        case ECLASS:
            startp = v->now;
            endp = scanplain(v);
            INSIST(startp < endp, REG_ECOLLATE);
            startc = element(v, startp, endp);
            cv = eclass(v, startc, (v->cflags & REG_ICASE));
            subcolorcvec(v, cv, lp, rp);
            return;
        case CCLASS:
            startp = v->now;
            endp = scanplain(v);
            INSIST(startp < endp, REG_ECTYPE);
            cls = lookupcclass(v, startp, endp);
            charclass(v, cls, lp, rp);
            return;
        case CCLASSS:
            charclass(v, (enum char_classes) v->nextvalue, lp, rp);
            NEXT();
            return;
        case CCLASSC:
            // Defer complemented character class processing
            have_cclassc[v->nextvalue] = true;
            NEXT();
            return;
        default:
            ERR(REG_ASSERT);
            return;
    }

    // Handle ranges if present
    if (SEE(RANGE)) {
        NEXT();
        switch (v->nexttype) {
            case PLAIN:
            case RANGE:
                endc = v->nextvalue;
                NEXT();
                break;
            case COLLEL:
                startp = v->now;
                endp = scanplain(v);
                INSIST(startp < endp, REG_ECOLLATE);
                endc = element(v, startp, endp);
                break;
            default:
                ERR(REG_ERANGE);
                return;
        }
    } else {
        endc = startc;
    }

    // Create range and add to NFA
    if (startc != endc)
        NOTE(REG_UUNPORT);
    cv = range(v, startc, endc, (v->cflags & REG_ICASE));
    subcolorcvec(v, cv, lp, rp);
}
```