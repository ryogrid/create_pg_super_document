# word_distance

## Location
[src/backend/utils/adt/tsrank.c:44-52](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsrank.c#L44-L52)

## Overview
Calculates a weight factor for word collocation based on the distance between words in text search ranking operations.

## Definition

```c
static float4
word_distance(int32 w)
```
## Detailed Description
The  function computes a weight coefficient used in PostgreSQL's text search ranking algorithm. It takes the distance between words as input and returns a normalized weight factor that decreases as the distance increases. The function implements an exponential decay formula to give higher weights to words that appear closer together, which is a fundamental principle in text search relevance scoring.

The function uses a mathematical formula that provides:
- Maximum weight (close to 1.0) for words that are very close together
- Rapidly decreasing weights as distance increases
- A floor value of 1e-30 for distances greater than 100

## Parameters / Member Variables
-  04:49:01 up 21:14,  0 users,  load average: 0.54, 0.56, 0.55
USER     TTY      FROM             LOGIN@   IDLE   JCPU   PCPU WHAT: The distance (in word positions) between two words in the text

## Dependencies
- Functions called/Symbols referenced:
  -  (mathematical exponential function)
  -  (PostgreSQL's 4-byte floating point type)
- Called from (representative examples):
  -  (src/backend/utils/adt/tsrank.c:267)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the tsrank.c file
- The function implements a specific mathematical model for text search relevance where word proximity significantly affects scoring
- For distances greater than 100 word positions, the function returns a minimal weight (1e-30), effectively treating such distant words as having negligible correlation
- The exponential decay formula  is tuned to provide reasonable weight distribution for typical text search scenarios