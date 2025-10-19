# evalStandardFunc

## Location
[src/bin/pgbench/pgbench.c:2249-2820](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L2249-L2820)

## Overview
A comprehensive function evaluation engine that handles eager evaluation of all standard pgbench functions, including arithmetic, logical, comparison, mathematical, random, and utility operations.

## Definition
```c
static bool evalStandardFunc(CState *st, PgBenchFunction func, PgBenchExprLink *args, PgBenchValue *retval)
```

## Detailed Description
The `evalStandardFunc` function implements eager evaluation for all non-lazy pgbench functions. It first evaluates all function arguments into a local array, then dispatches to appropriate handlers based on the function type. The function supports extensive type coercion between integers, doubles, and booleans, with proper overflow checking for integer arithmetic. It handles overloaded operators that work on both integer and floating-point types, mathematical functions, random number generation with various distributions, hashing functions, and utility operations like debugging and type casting.

## Parameters / Member Variables
- `st`: Pointer to the current client state (`CState`) containing execution context and random state
- `func`: The `PgBenchFunction` enum value specifying which function to evaluate
- `args`: Linked list of expression arguments (`PgBenchExprLink`) to be eagerly evaluated
- `retval`: Pointer to `PgBenchValue` where the result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - `[evaluateExpr](evaluateExpr.md)` (recursive expression evaluation)
  - `[coerceToInt](../c/coerceToInt.md)`, `coerceToDouble`, `coerceToBool` (type conversions)
  - `[setIntValue](../s/setIntValue.md)`, `setDoubleValue`, `setBoolValue`, `setNullValue` (result setters)
  - Mathematical functions: `sqrt`, `log`, `exp`, `pow`
  - Random generators: `getrand`, `getGaussianRand`, `getZipfianRand`, `getExponentialRand`
  - [Hash](../H/Hash.md) functions: `getHashMurmur2`, `getHashFnv1a`
  - `[permute](../p/permute.md)` (permutation function)
  - Overflow-safe arithmetic: `pg_add_s64_overflow`, `pg_sub_s64_overflow`, `pg_mul_s64_overflow`
  - Various `PGBENCH_*` enum constants for function types
- Called from (representative examples):
  - `[evalFunc](evalFunc.md)`

## Notes and Other Information
- This is a static function with internal linkage, only accessible within pgbench.c
- Handles comprehensive NULL propagation - most functions return NULL if any argument is NULL (except IS and DEBUG)
- Implements proper SQL-like semantics for comparison and arithmetic operations
- Supports type promotion where operations involving doubles return doubles
- Includes extensive error checking for division by zero, parameter ranges, and integer overflow
- The function uses a local array `vargs[MAX_FARGS]` to store evaluated arguments before processing
- Handles variable-argument functions like LEAST/GREATEST that can accept multiple parameters
- Critical component of pgbench's expression evaluation system, handling the majority of built-in functions

## Simplified Source

```c
static bool evalStandardFunc(CState *st, PgBenchFunction func, PgBenchExprLink *args, PgBenchValue *retval) {
    // Evaluate all function arguments into local array
    PgBenchValue vargs[MAX_FARGS];
    int nargs = 0;
    bool has_null = false;

    for (PgBenchExprLink *l = args; l != NULL && nargs < MAX_FARGS; l = l->next, nargs++) {
        if (!evaluateExpr(st, l->expr, &vargs[nargs]))
            return false;
        has_null |= (vargs[nargs].type == PGBT_NULL);
    }

    // Most functions return NULL if any argument is NULL (except IS and DEBUG)
    if (has_null && func != PGBENCH_IS && func != PGBENCH_DEBUG) {
        setNullValue(retval);
        return true;
    }

    // Dispatch to specific function handlers
    switch (func) {
        // Arithmetic operators (+, -, *, /, %, etc.)
        case PGBENCH_ADD:
        case PGBENCH_SUB:
        case PGBENCH_MUL:
        case PGBENCH_DIV:
        case PGBENCH_MOD: {
            PgBenchValue *left = &vargs[0], *right = &vargs[1];

            // Use double arithmetic if either operand is double (except MOD)
            if ((left->type == PGBT_DOUBLE || right->type == PGBT_DOUBLE) && func != PGBENCH_MOD) {
                double ld, rd;
                if (!coerceToDouble(left, &ld) || !coerceToDouble(right, &rd))
                    return false;

                switch (func) {
                    case PGBENCH_ADD: setDoubleValue(retval, ld + rd); break;
                    case PGBENCH_SUB: setDoubleValue(retval, ld - rd); break;
                    case PGBENCH_MUL: setDoubleValue(retval, ld * rd); break;
                    case PGBENCH_DIV: setDoubleValue(retval, ld / rd); break;
                }
            } else {
                // Integer arithmetic with overflow checking
                int64 li, ri, result;
                if (!coerceToInt(left, &li) || !coerceToInt(right, &ri))
                    return false;

                switch (func) {
                    case PGBENCH_ADD:
                        if (pg_add_s64_overflow(li, ri, &result)) {
                            pg_log_error("bigint add out of range");
                            return false;
                        }
                        setIntValue(retval, result);
                        break;
                    case PGBENCH_SUB:
                        if (pg_sub_s64_overflow(li, ri, &result)) {
                            pg_log_error("bigint sub out of range");
                            return false;
                        }
                        setIntValue(retval, result);
                        break;
                    case PGBENCH_MUL:
                        if (pg_mul_s64_overflow(li, ri, &result)) {
                            pg_log_error("bigint mul out of range");
                            return false;
                        }
                        setIntValue(retval, result);
                        break;
                    case PGBENCH_DIV:
                    case PGBENCH_MOD:
                        if (ri == 0) {
                            pg_log_error("division by zero");
                            return false;
                        }
                        if (func == PGBENCH_DIV)
                            setIntValue(retval, li / ri);
                        else
                            setIntValue(retval, li % ri);
                        break;
                }
            }
            return true;
        }

        // Comparison operators (=, <>, <=, <)
        case PGBENCH_EQ:
        case PGBENCH_NE:
        case PGBENCH_LE:
        case PGBENCH_LT: {
            PgBenchValue *left = &vargs[0], *right = &vargs[1];

            if (left->type == PGBT_DOUBLE || right->type == PGBT_DOUBLE) {
                double ld, rd;
                if (!coerceToDouble(left, &ld) || !coerceToDouble(right, &rd))
                    return false;

                switch (func) {
                    case PGBENCH_EQ: setBoolValue(retval, ld == rd); break;
                    case PGBENCH_NE: setBoolValue(retval, ld != rd); break;
                    case PGBENCH_LE: setBoolValue(retval, ld <= rd); break;
                    case PGBENCH_LT: setBoolValue(retval, ld < rd); break;
                }
            } else {
                int64 li, ri;
                if (!coerceToInt(left, &li) || !coerceToInt(right, &ri))
                    return false;

                switch (func) {
                    case PGBENCH_EQ: setBoolValue(retval, li == ri); break;
                    case PGBENCH_NE: setBoolValue(retval, li != ri); break;
                    case PGBENCH_LE: setBoolValue(retval, li <= ri); break;
                    case PGBENCH_LT: setBoolValue(retval, li < ri); break;
                }
            }
            return true;
        }

        // Bitwise operators (&, |, ^, <<, >>)
        case PGBENCH_BITAND:
        case PGBENCH_BITOR:
        case PGBENCH_BITXOR:
        case PGBENCH_LSHIFT:
        case PGBENCH_RSHIFT: {
            int64 li, ri;
            if (!coerceToInt(&vargs[0], &li) || !coerceToInt(&vargs[1], &ri))
                return false;

            switch (func) {
                case PGBENCH_BITAND: setIntValue(retval, li & ri); break;
                case PGBENCH_BITOR:  setIntValue(retval, li | ri); break;
                case PGBENCH_BITXOR: setIntValue(retval, li ^ ri); break;
                case PGBENCH_LSHIFT: setIntValue(retval, li << ri); break;
                case PGBENCH_RSHIFT: setIntValue(retval, li >> ri); break;
            }
            return true;
        }

        // Logical NOT
        case PGBENCH_NOT: {
            bool b;
            if (!coerceToBool(&vargs[0], &b))
                return false;
            setBoolValue(retval, !b);
            return true;
        }

        // Mathematical constants and functions
        case PGBENCH_PI:
            setDoubleValue(retval, M_PI);
            return true;

        case PGBENCH_ABS: {
            PgBenchValue *varg = &vargs[0];
            if (varg->type == PGBT_INT) {
                int64 i = varg->u.ival;
                setIntValue(retval, i < 0 ? -i : i);
            } else {
                double d = varg->u.dval;
                setDoubleValue(retval, d < 0.0 ? -d : d);
            }
            return true;
        }

        case PGBENCH_SQRT:
        case PGBENCH_LN:
        case PGBENCH_EXP: {
            double dval;
            if (!coerceToDouble(&vargs[0], &dval))
                return false;

            switch (func) {
                case PGBENCH_SQRT: dval = sqrt(dval); break;
                case PGBENCH_LN:   dval = log(dval); break;
                case PGBENCH_EXP:  dval = exp(dval); break;
            }
            setDoubleValue(retval, dval);
            return true;
        }

        case PGBENCH_POW: {
            double ld, rd;
            if (!coerceToDouble(&vargs[0], &ld) || !coerceToDouble(&vargs[1], &rd))
                return false;
            setDoubleValue(retval, pow(ld, rd));
            return true;
        }

        // Type casting functions
        case PGBENCH_INT: {
            int64 ival;
            if (!coerceToInt(&vargs[0], &ival))
                return false;
            setIntValue(retval, ival);
            return true;
        }

        case PGBENCH_DOUBLE: {
            double dval;
            if (!coerceToDouble(&vargs[0], &dval))
                return false;
            setDoubleValue(retval, dval);
            return true;
        }

        // Random number generation
        case PGBENCH_RANDOM: {
            int64 imin, imax;
            if (!coerceToInt(&vargs[0], &imin) || !coerceToInt(&vargs[1], &imax))
                return false;

            if (imin > imax) {
                pg_log_error("empty range given to random");
                return false;
            }

            setIntValue(retval, getrand(&st->cs_func_rs, imin, imax));
            return true;
        }

        // Hash functions
        case PGBENCH_HASH_FNV1A:
        case PGBENCH_HASH_MURMUR2: {
            int64 val, seed;
            if (!coerceToInt(&vargs[0], &val) || !coerceToInt(&vargs[1], &seed))
                return false;

            if (func == PGBENCH_HASH_MURMUR2)
                setIntValue(retval, getHashMurmur2(val, seed));
            else
                setIntValue(retval, getHashFnv1a(val, seed));
            return true;
        }

        // Debug function (special case - handles NULL arguments)
        case PGBENCH_DEBUG: {
            PgBenchValue *varg = &vargs[0];
            fprintf(stderr, "debug(script=%d,command=%d): ", st->use_file, st->command + 1);

            if (varg->type == PGBT_NULL)
                fprintf(stderr, "null\n");
            else if (varg->type == PGBT_BOOLEAN)
                fprintf(stderr, "boolean %s\n", varg->u.bval ? "true" : "false");
            else if (varg->type == PGBT_INT)
                fprintf(stderr, "int " INT64_FORMAT "\n", varg->u.ival);
            else if (varg->type == PGBT_DOUBLE)
                fprintf(stderr, "double %.*g\n", DBL_DIG, varg->u.dval);

            *retval = *varg;
            return true;
        }

        // Additional functions (LEAST, GREATEST, random variants, etc.) would follow similar patterns...
        default:
            Assert(0);  // Should never reach here
            return false;
    }
}
```