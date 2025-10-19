# d2d

## Location
[src/common/d2s.c:346-630](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/d2s.c#L346-L630)

## Overview
Converts IEEE 754 double-precision floating-point components (mantissa and exponent) into the shortest decimal representation that can round-trip back to the original double value.

## Definition
```c
static inline floating_decimal_64 d2d(const uint64 ieeeMantissa, const uint32 ieeeExponent)
```

## Detailed Description
This function implements the core algorithm for converting IEEE 754 double-precision floating-point numbers to their shortest decimal representation. It takes the raw mantissa and exponent components of a double and produces a decimal representation with the minimum number of digits necessary to uniquely identify the original floating-point value.

The algorithm works in several key steps:
1. Normalizes the input mantissa and exponent, accounting for subnormal numbers
2. Determines the interval of legal decimal representations using bounds computation
3. Converts to decimal using 128-bit arithmetic with precomputed power-of-5 tables
4. Finds the shortest representation within the legal interval using digit removal optimization

The implementation includes sophisticated optimizations such as:
- Special handling for trailing zeros to minimize output length
- Fast division by 10 and 100 using multiplication and shifting
- Conditional logic to handle different rounding scenarios
- Performance optimizations that remove multiple digits at once when possible

## Parameters / Member Variables
- `ieeeMantissa`: The 52-bit mantissa portion of the IEEE 754 double (without the implicit leading 1 for normal numbers)
- `ieeeExponent`: The 11-bit exponent portion of the IEEE 754 double (biased by 1023)

## Dependencies
- Functions called/Symbols referenced:
  - [log10Pow2](../l/log10Pow2.md): Computes floor(log₁₀(2^e))
  - [log10Pow5](../l/log10Pow5.md): Computes floor(log₁₀(5^e))  
  - [pow5bits](../p/pow5bits.md): Computes number of bits needed for 5^e
  - [mulShiftAll](../m/mulShiftAll.md): Performs 128-bit multiplication with shifting
  - [div5](div5.md), div10, div100: Fast division functions
  - [multipleOfPowerOf5](../m/multipleOfPowerOf5.md): Checks divisibility by powers of 5
  - [multipleOfPowerOf2](../m/multipleOfPowerOf2.md): Checks divisibility by powers of 2
  - DOUBLE_BIAS, DOUBLE_MANTISSA_BITS: IEEE 754 double constants
  - DOUBLE_POW5_INV_SPLIT, DOUBLE_POW5_SPLIT: Precomputed power tables
  - [floating_decimal_64](../f/floating_decimal_64.md): Return type structure
- Called from (representative examples):
  - [double_to_shortest_decimal_bufn](double_to_shortest_decimal_bufn.md)

## Notes and Other Information
- The function is marked as `static inline` for performance optimization
- Uses conditional compilation with STRICTLY_SHORTEST to control rounding behavior
- Implements the Ryu algorithm for fast and accurate floating-point to string conversion
- Handles both normal and subnormal IEEE 754 double values
- The algorithm guarantees the shortest decimal representation that round-trips correctly
- Performance is optimized for common cases, with the general case handling rare scenarios (~0.7%)
- Contains detailed comments explaining the mathematical reasoning behind various optimizations

## Simplified Source

```c
static inline floating_decimal_64 d2d(const uint64 ieeeMantissa, const uint32 ieeeExponent) {
    // Step 1: Normalize mantissa and exponent
    int32 e2;
    uint64 m2;

    if (ieeeExponent == 0) {
        // Subnormal number
        e2 = 1 - DOUBLE_BIAS - DOUBLE_MANTISSA_BITS - 2;
        m2 = ieeeMantissa;
    } else {
        // Normal number - add implicit leading 1
        e2 = ieeeExponent - DOUBLE_BIAS - DOUBLE_MANTISSA_BITS - 2;
        m2 = (UINT64CONST(1) << DOUBLE_MANTISSA_BITS) | ieeeMantissa;
    }

    // Step 2: Determine bounds for legal decimal representations
    const uint64 mv = 4 * m2;
    const uint32 mmShift = ieeeMantissa != 0 || ieeeExponent <= 1;

    // Step 3: Convert to decimal using 128-bit arithmetic
    uint64 vr, vp, vm;
    int32 e10;
    bool vmIsTrailingZeros = false;
    bool vrIsTrailingZeros = false;

    if (e2 >= 0) {
        // Positive exponent path
        const uint32 q = log10Pow2(e2) - (e2 > 3);
        const int32 k = DOUBLE_POW5_INV_BITCOUNT + pow5bits(q) - 1;
        const int32 i = -e2 + q + k;
        e10 = q;

        vr = mulShiftAll(m2, DOUBLE_POW5_INV_SPLIT[q], i, &vp, &vm, mmShift);

        // Check for trailing zeros if q <= 21
        if (q <= 21) {
            const uint32 mvMod5 = (uint32)(mv - 5 * div5(mv));
            if (mvMod5 == 0) {
                vrIsTrailingZeros = multipleOfPowerOf5(mv, q);
            }
        }
    } else {
        // Negative exponent path
        const uint32 q = log10Pow5(-e2) - (-e2 > 1);
        const int32 i = -e2 - q;
        const int32 k = pow5bits(i) - DOUBLE_POW5_BITCOUNT;
        const int32 j = q - k;
        e10 = q + e2;

        vr = mulShiftAll(m2, DOUBLE_POW5_SPLIT[i], j, &vp, &vm, mmShift);

        // Handle trailing zeros for small q
        if (q <= 1) {
            vrIsTrailingZeros = true;
        } else if (q < 63) {
            vrIsTrailingZeros = multipleOfPowerOf2(mv, q - 1);
        }
    }

    // Step 4: Find shortest representation by removing digits
    uint32 removed = 0;
    uint8 lastRemovedDigit = 0;
    uint64 output;

    if (vmIsTrailingZeros || vrIsTrailingZeros) {
        // General case with trailing zero handling
        while (true) {
            const uint64 vpDiv10 = div10(vp);
            const uint64 vmDiv10 = div10(vm);
            if (vpDiv10 <= vmDiv10) break;

            const uint64 vrDiv10 = div10(vr);
            lastRemovedDigit = (uint8)(vr - 10 * vrDiv10);

            vr = vrDiv10;
            vp = vpDiv10;
            vm = vmDiv10;
            removed++;
        }

        // Rounding logic for trailing zeros
        output = vr + ((vr == vm) || lastRemovedDigit >= 5);
    } else {
        // Common case optimization (~99.3%)
        bool roundUp = false;

        // Try removing two digits at once
        const uint64 vpDiv100 = div100(vp);
        const uint64 vmDiv100 = div100(vm);
        if (vpDiv100 > vmDiv100) {
            const uint64 vrDiv100 = div100(vr);
            const uint32 vrMod100 = (uint32)(vr - 100 * vrDiv100);
            roundUp = vrMod100 >= 50;
            vr = vrDiv100;
            vp = vpDiv100;
            vm = vmDiv100;
            removed += 2;
        }

        // Remove remaining digits one by one
        while (true) {
            const uint64 vpDiv10 = div10(vp);
            const uint64 vmDiv10 = div10(vm);
            if (vpDiv10 <= vmDiv10) break;

            const uint64 vrDiv10 = div10(vr);
            const uint32 vrMod10 = (uint32)(vr - 10 * vrDiv10);
            roundUp = vrMod10 >= 5;

            vr = vrDiv10;
            vp = vpDiv10;
            vm = vmDiv10;
            removed++;
        }

        output = vr + (vr == vm || roundUp);
    }

    // Return result
    floating_decimal_64 fd;
    fd.exponent = e10 + removed;
    fd.mantissa = output;
    return fd;
}
```