# init_degree_constants

## Location
[src/backend/utils/adt/float.c:2012-2023](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L2012-L2023)

## Overview
A static initialization function that computes and caches trigonometric constants for degree-based trigonometric functions in PostgreSQL.

## Definition

```c
static void
init_degree_constants(void)
```
## Detailed Description
The  function initializes cached constants (sin_30, one_minus_cos_60, asin_0_5, acos_0_5, atan_1_0, tan_45, cot_45) used by PostgreSQL's degree-based trigonometric functions. This complex initialization method exists to ensure exact results by preventing compilers from precomputing trigonometric expressions using different sin/cos functions than those used at runtime. The function also addresses potential compiler optimizations that could rearrange expressions or use wider registers than standard double precision, which could affect numerical accuracy.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - sin (standard C library sine function)
  - cos (standard C library cosine function)  
  - asin (standard C library arcsine function)
  - acos (standard C library arccosine function)
  - atan (standard C library arctangent function)
  - [sind_q1](../s/sind_q1.md) (PostgreSQL degree-based sine function for first quadrant)
  - [cosd_q1](../c/cosd_q1.md) (PostgreSQL degree-based cosine function for first quadrant)
  - RADIANS_PER_DEGREE (conversion constant)
- Called from (representative examples):
  - INIT_DEGREE_CONSTANTS (macro at src/backend/utils/adt/float.c:2027)

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:2012-2023
- Uses a "Rube-Goldberg" approach to prevent compiler optimizations that could affect numerical precision
- Constants are computed from variables (degree_c_thirty, degree_c_sixty, etc.) that the compiler cannot assume are constants
- Sets degree_consts_set flag to true when initialization is complete
- The complex design ensures that degree-based trig functions return exactly 1.0 for expressions like sin(30°)/sin(30°)
- Uses volatile temporary variables in calling code to ensure proper rounding on machines with wide float registers

## Simplified Source

```c
static void init_degree_constants(void) {
    // Compute trigonometric constants for degree-based functions
    // Uses variables instead of literals to prevent compiler optimization
    sin_30 = sin(degree_c_thirty * RADIANS_PER_DEGREE);
    one_minus_cos_60 = 1.0 - cos(degree_c_sixty * RADIANS_PER_DEGREE);
    asin_0_5 = asin(degree_c_one_half);
    acos_0_5 = acos(degree_c_one_half);
    atan_1_0 = atan(degree_c_one);

    // Compute tan and cot using degree-based sine/cosine functions
    tan_45 = sind_q1(degree_c_forty_five) / cosd_q1(degree_c_forty_five);
    cot_45 = cosd_q1(degree_c_forty_five) / sind_q1(degree_c_forty_five);

    degree_consts_set = true;
}
```