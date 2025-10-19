# checkcondition_gin

## Location
[src/backend/utils/adt/tsginidx.c:183-213](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsginidx.c#L183-L213)

## Overview
A callback function for TS_execute that determines whether a tsquery operand matches GIN index data during text search consistency checks.

## Definition

```c
static TSTernaryValue
checkcondition_gin(void *checkval, QueryOperand *val, ExecPhraseData *data)
```
## Detailed Description
This function serves as a specialized callback for PostgreSQL's text search execution engine (TS_execute) when working with GIN indexes. It evaluates individual query operands against indexed TSVector data to determine match conditions during query consistency checking.

The function performs several key operations:
1. **Operand Mapping**: Converts query item numbers to corresponding entry numbers using a pre-built mapping
2. **Presence Checking**: Determines if the current operand exists in the indexed data
3. **Weight and Position Handling**: Downgrades definitive matches to "maybe" when weight or position information is required, forcing more detailed rechecking
4. **Value Conversion**: Translates between GIN ternary values and TS ternary values

This callback is crucial for the two-phase checking process in GIN text search: first determining basic presence/absence, then potentially requiring detailed recheck for complex conditions.

## Parameters / Member Variables
- `*checkval`:  - Pointer to GinChkVal structure containing check context
- `*val`:  - Query operand being evaluated for matching
- `*data`:  - Phrase execution data (NULL if position info not needed)
## Dependencies
- Functions called/Symbols referenced:
  -  - Structure containing check array and mapping information
  -  - Text search query operand structure
  -  - [Query](../Q/Query.md) tree item type for pointer arithmetic
  -  - Data structure for phrase execution context
  -  - GIN ternary logic values (TRUE, FALSE, MAYBE)
  -  - Text search ternary logic values
  -  - GIN true value constant
  -  - GIN maybe value constant
- Called from (representative examples):
  -  - Main consistency checking function
  -  - Ternary consistency checking function

## Notes and Other Information
- Returns TSTernaryValue result indicating match status (TRUE/FALSE/MAYBE)
- Converts GIN_TRUE to GIN_MAYBE when weight constraints or position data are required
- Relies on equivalent value assignments between GinTernaryValue and TSTernaryValue enums
- Used in two-phase GIN checking: initial fast check followed by potential detailed recheck
- Critical for supporting advanced text search features like weight-based matching and phrase queries
- Static function scope limits visibility to tsginidx.c compilation unit
- Part of PostgreSQL's extensible text search architecture

## Simplified Source

```c
static TSTernaryValue
checkcondition_gin(void *checkval, QueryOperand *val, ExecPhraseData *data)
{
    GinChkVal *gcv = (GinChkVal *) checkval;
    int j;
    GinTernaryValue result;

    // Map query item to operand number using pre-built mapping
    j = gcv->map_item_operand[((QueryItem *) val) - gcv->first_item];

    // Get match result for this operand from check array
    result = gcv->check[j];

    // Downgrade TRUE to MAYBE if weight or position info needed
    if (result == GIN_TRUE) {
        if (val->weight != 0 || data != NULL)
            result = GIN_MAYBE;  // Force recheck for complex conditions
    }

    // Convert GIN ternary value to TS ternary value (equivalent enums)
    return (TSTernaryValue) result;
}
```