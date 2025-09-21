9.3. Mathematical Functions and Operators  
---  
[Prev](functions-comparison.md "9.2. Comparison Functions and Operators") | [Up](functions.md "Chapter 9. Functions and Operators")| Chapter 9. Functions and Operators| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](functions-string.md "9.4. String Functions and Operators")  
  
* * *

## 9.3. Mathematical Functions and Operators #

Mathematical operators are provided for many PostgreSQL types. For types without standard mathematical conventions (e.g., date/time types) we describe the actual behavior in subsequent sections. 

[Table 9.4](functions-math.md#FUNCTIONS-MATH-OP-TABLE "Table 9.4. Mathematical Operators") shows the mathematical operators that are available for the standard numeric types. Unless otherwise noted, operators shown as accepting _`numeric_type`_ are available for all the types `smallint`, `integer`, `bigint`, `numeric`, `real`, and `double precision`. Operators shown as accepting _`integral_type`_ are available for the types `smallint`, `integer`, and `bigint`. Except where noted, each form of an operator returns the same data type as its argument(s). Calls involving multiple argument data types, such as `integer` `+` `numeric`, are resolved by using the type appearing later in these lists. 

**Table 9.4. Mathematical Operators**

Operator  Description  Example(s)   
---  
_`numeric_type`_ `+` _`numeric_type`_ → `_`numeric_type`_` Addition  `2 + 3` → `5`  
`+` _`numeric_type`_ → `_`numeric_type`_` Unary plus (no operation)  `+ 3.5` → `3.5`  
_`numeric_type`_ `-` _`numeric_type`_ → `_`numeric_type`_` Subtraction  `2 - 3` → `-1`  
`-` _`numeric_type`_ → `_`numeric_type`_` Negation  `- (-4)` → `4`  
_`numeric_type`_ `*` _`numeric_type`_ → `_`numeric_type`_` Multiplication  `2 * 3` → `6`  
_`numeric_type`_ `/` _`numeric_type`_ → `_`numeric_type`_` Division (for integral types, division truncates the result towards zero)  `5.0 / 2` → `2.5000000000000000` `5 / 2` → `2` `(-5) / 2` → `-2`  
_`numeric_type`_ `%` _`numeric_type`_ → `_`numeric_type`_` Modulo (remainder); available for `smallint`, `integer`, `bigint`, and `numeric` `5 % 4` → `1`  
`numeric` `^` `numeric` → `numeric` `double precision` `^` `double precision` → `double precision` Exponentiation  `2 ^ 3` → `8` Unlike typical mathematical practice, multiple uses of `^` will associate left to right by default:  `2 ^ 3 ^ 3` → `512` `2 ^ (3 ^ 3)` → `134217728`  
`|/` `double precision` → `double precision` Square root  `|/ 25.0` → `5`  
`||/` `double precision` → `double precision` Cube root  `||/ 64.0` → `4`  
`@` _`numeric_type`_ → `_`numeric_type`_` Absolute value  `@ -5.0` → `5.0`  
_`integral_type`_ `&` _`integral_type`_ → `_`integral_type`_` Bitwise AND  `91 & 15` → `11`  
_`integral_type`_ `|` _`integral_type`_ → `_`integral_type`_` Bitwise OR  `32 | 3` → `35`  
_`integral_type`_ `#` _`integral_type`_ → `_`integral_type`_` Bitwise exclusive OR  `17 # 5` → `20`  
`~` _`integral_type`_ → `_`integral_type`_` Bitwise NOT  `~1` → `-2`  
_`integral_type`_ `<<` `integer` → `_`integral_type`_` Bitwise shift left  `1 << 4` → `16`  
_`integral_type`_ `>>` `integer` → `_`integral_type`_` Bitwise shift right  `8 >> 2` → `2`  
  
  


[Table 9.5](functions-math.md#FUNCTIONS-MATH-FUNC-TABLE "Table 9.5. Mathematical Functions") shows the available mathematical functions. Many of these functions are provided in multiple forms with different argument types. Except where noted, any given form of a function returns the same data type as its argument(s); cross-type cases are resolved in the same way as explained above for operators. The functions working with `double precision` data are mostly implemented on top of the host system's C library; accuracy and behavior in boundary cases can therefore vary depending on the host system. 

**Table 9.5. Mathematical Functions**

Function  Description  Example(s)   
---  
`abs` ( _`numeric_type`_ ) → `_`numeric_type`_` Absolute value  `abs(-17.4)` → `17.4`  
`cbrt` ( `double precision` ) → `double precision` Cube root  `cbrt(64.0)` → `4`  
`ceil` ( `numeric` ) → `numeric` `ceil` ( `double precision` ) → `double precision` Nearest integer greater than or equal to argument  `ceil(42.2)` → `43` `ceil(-42.8)` → `-42`  
`ceiling` ( `numeric` ) → `numeric` `ceiling` ( `double precision` ) → `double precision` Nearest integer greater than or equal to argument (same as `ceil`)  `ceiling(95.3)` → `96`  
`degrees` ( `double precision` ) → `double precision` Converts radians to degrees  `degrees(0.5)` → `28.64788975654116`  
`div` ( _`y`_ `numeric`, _`x`_ `numeric` ) → `numeric` Integer quotient of _`y`_ /_`x`_ (truncates towards zero)  `div(9, 4)` → `2`  
`erf` ( `double precision` ) → `double precision` Error function  `erf(1.0)` → `0.8427007929497149`  
`erfc` ( `double precision` ) → `double precision` Complementary error function (`1 - erf(x)`, without loss of precision for large inputs)  `erfc(1.0)` → `0.15729920705028513`  
`exp` ( `numeric` ) → `numeric` `exp` ( `double precision` ) → `double precision` Exponential (`e` raised to the given power)  `exp(1.0)` → `2.7182818284590452`  
`factorial` ( `bigint` ) → `numeric` Factorial  `factorial(5)` → `120`  
`floor` ( `numeric` ) → `numeric` `floor` ( `double precision` ) → `double precision` Nearest integer less than or equal to argument  `floor(42.8)` → `42` `floor(-42.8)` → `-43`  
`gcd` ( _`numeric_type`_ , _`numeric_type`_ ) → `_`numeric_type`_` Greatest common divisor (the largest positive number that divides both inputs with no remainder); returns `0` if both inputs are zero; available for `integer`, `bigint`, and `numeric` `gcd(1071, 462)` → `21`  
`lcm` ( _`numeric_type`_ , _`numeric_type`_ ) → `_`numeric_type`_` Least common multiple (the smallest strictly positive number that is an integral multiple of both inputs); returns `0` if either input is zero; available for `integer`, `bigint`, and `numeric` `lcm(1071, 462)` → `23562`  
`ln` ( `numeric` ) → `numeric` `ln` ( `double precision` ) → `double precision` Natural logarithm  `ln(2.0)` → `0.6931471805599453`  
`log` ( `numeric` ) → `numeric` `log` ( `double precision` ) → `double precision` Base 10 logarithm  `log(100)` → `2`  
`log10` ( `numeric` ) → `numeric` `log10` ( `double precision` ) → `double precision` Base 10 logarithm (same as `log`)  `log10(1000)` → `3`  
`log` ( _`b`_ `numeric`, _`x`_ `numeric` ) → `numeric` Logarithm of _`x`_ to base _`b`_ `log(2.0, 64.0)` → `6.0000000000000000`  
`min_scale` ( `numeric` ) → `integer` Minimum scale (number of fractional decimal digits) needed to represent the supplied value precisely  `min_scale(8.4100)` → `2`  
`mod` ( _`y`_ _`numeric_type`_ , _`x`_ _`numeric_type`_ ) → `_`numeric_type`_` Remainder of _`y`_ /_`x`_ ; available for `smallint`, `integer`, `bigint`, and `numeric` `mod(9, 4)` → `1`  
`pi` ( ) → `double precision` Approximate value of π `pi()` → `3.141592653589793`  
`power` ( _`a`_ `numeric`, _`b`_ `numeric` ) → `numeric` `power` ( _`a`_ `double precision`, _`b`_ `double precision` ) → `double precision` _`a`_ raised to the power of _`b`_ `power(9, 3)` → `729`  
`radians` ( `double precision` ) → `double precision` Converts degrees to radians  `radians(45.0)` → `0.7853981633974483`  
`round` ( `numeric` ) → `numeric` `round` ( `double precision` ) → `double precision` Rounds to nearest integer. For `numeric`, ties are broken by rounding away from zero. For `double precision`, the tie-breaking behavior is platform dependent, but “round to nearest even” is the most common rule.  `round(42.4)` → `42`  
`round` ( _`v`_ `numeric`, _`s`_ `integer` ) → `numeric` Rounds _`v`_ to _`s`_ decimal places. Ties are broken by rounding away from zero.  `round(42.4382, 2)` → `42.44` `round(1234.56, -1)` → `1230`  
`scale` ( `numeric` ) → `integer` Scale of the argument (the number of decimal digits in the fractional part)  `scale(8.4100)` → `4`  
`sign` ( `numeric` ) → `numeric` `sign` ( `double precision` ) → `double precision` Sign of the argument (-1, 0, or +1)  `sign(-8.4)` → `-1`  
`sqrt` ( `numeric` ) → `numeric` `sqrt` ( `double precision` ) → `double precision` Square root  `sqrt(2)` → `1.4142135623730951`  
`trim_scale` ( `numeric` ) → `numeric` Reduces the value's scale (number of fractional decimal digits) by removing trailing zeroes  `trim_scale(8.4100)` → `8.41`  
`trunc` ( `numeric` ) → `numeric` `trunc` ( `double precision` ) → `double precision` Truncates to integer (towards zero)  `trunc(42.8)` → `42` `trunc(-42.8)` → `-42`  
`trunc` ( _`v`_ `numeric`, _`s`_ `integer` ) → `numeric` Truncates _`v`_ to _`s`_ decimal places  `trunc(42.4382, 2)` → `42.43`  
`width_bucket` ( _`operand`_ `numeric`, _`low`_ `numeric`, _`high`_ `numeric`, _`count`_ `integer` ) → `integer` `width_bucket` ( _`operand`_ `double precision`, _`low`_ `double precision`, _`high`_ `double precision`, _`count`_ `integer` ) → `integer` Returns the number of the bucket in which _`operand`_ falls in a histogram having _`count`_ equal-width buckets spanning the range _`low`_ to _`high`_. The buckets have inclusive lower bounds and exclusive upper bounds. Returns `0` for an input less than _`low`_ , or `_`count`_ +1` for an input greater than or equal to _`high`_. If _`low`_ > _`high`_ , the behavior is mirror-reversed, with bucket `1` now being the one just below _`low`_ , and the inclusive bounds now being on the upper side.  `width_bucket(5.35, 0.024, 10.06, 5)` → `3` `width_bucket(9, 10, 0, 10)` → `2`  
`width_bucket` ( _`operand`_ `anycompatible`, _`thresholds`_ `anycompatiblearray` ) → `integer` Returns the number of the bucket in which _`operand`_ falls given an array listing the inclusive lower bounds of the buckets. Returns `0` for an input less than the first lower bound. _`operand`_ and the array elements can be of any type having standard comparison operators. The _`thresholds`_ array _must be sorted_ , smallest first, or unexpected results will be obtained.  `width_bucket(now(), array['yesterday', 'today', 'tomorrow']::timestamptz[])` → `2`  
  
  


[Table 9.6](functions-math.md#FUNCTIONS-MATH-RANDOM-TABLE "Table 9.6. Random Functions") shows functions for generating random numbers. 

**Table 9.6. Random Functions**

Function  Description  Example(s)   
---  
`random` ( ) → `double precision` Returns a random value in the range 0.0 <= x < 1.0  `random()` → `0.897124072839091`  
`random` ( _`min`_ `integer`, _`max`_ `integer` ) → `integer` `random` ( _`min`_ `bigint`, _`max`_ `bigint` ) → `bigint` `random` ( _`min`_ `numeric`, _`max`_ `numeric` ) → `numeric` Returns a random value in the range _`min`_ <= x <= _`max`_. For type `numeric`, the result will have the same number of fractional decimal digits as _`min`_ or _`max`_ , whichever has more.  `random(1, 10)` → `7` `random(-0.499, 0.499)` → `0.347`  
`random_normal` ( [ _`mean`_ `double precision` [, _`stddev`_ `double precision` ]] ) → `double precision` Returns a random value from the normal distribution with the given parameters; _`mean`_ defaults to 0.0 and _`stddev`_ defaults to 1.0  `random_normal(0.0, 1.0)` → `0.051285419`  
`setseed` ( `double precision` ) → `void` Sets the seed for subsequent `random()` and `random_normal()` calls; argument must be between -1.0 and 1.0, inclusive  `setseed(0.12345)`  
  
  


The `random()` and `random_normal()` functions listed in [Table 9.6](functions-math.md#FUNCTIONS-MATH-RANDOM-TABLE "Table 9.6. Random Functions") use a deterministic pseudo-random number generator. It is fast but not suitable for cryptographic applications; see the [pgcrypto](pgcrypto.md "F.26. pgcrypto — cryptographic functions") module for a more secure alternative. If `setseed()` is called, the series of results of subsequent calls to these functions in the current session can be repeated by re-issuing `setseed()` with the same argument. Without any prior `setseed()` call in the same session, the first call to any of these functions obtains a seed from a platform-dependent source of random bits. 

[Table 9.7](functions-math.md#FUNCTIONS-MATH-TRIG-TABLE "Table 9.7. Trigonometric Functions") shows the available trigonometric functions. Each of these functions comes in two variants, one that measures angles in radians and one that measures angles in degrees. 

**Table 9.7. Trigonometric Functions**

Function  Description  Example(s)   
---  
`acos` ( `double precision` ) → `double precision` Inverse cosine, result in radians  `acos(1)` → `0`  
`acosd` ( `double precision` ) → `double precision` Inverse cosine, result in degrees  `acosd(0.5)` → `60`  
`asin` ( `double precision` ) → `double precision` Inverse sine, result in radians  `asin(1)` → `1.5707963267948966`  
`asind` ( `double precision` ) → `double precision` Inverse sine, result in degrees  `asind(0.5)` → `30`  
`atan` ( `double precision` ) → `double precision` Inverse tangent, result in radians  `atan(1)` → `0.7853981633974483`  
`atand` ( `double precision` ) → `double precision` Inverse tangent, result in degrees  `atand(1)` → `45`  
`atan2` ( _`y`_ `double precision`, _`x`_ `double precision` ) → `double precision` Inverse tangent of _`y`_ /_`x`_ , result in radians  `atan2(1, 0)` → `1.5707963267948966`  
`atan2d` ( _`y`_ `double precision`, _`x`_ `double precision` ) → `double precision` Inverse tangent of _`y`_ /_`x`_ , result in degrees  `atan2d(1, 0)` → `90`  
`cos` ( `double precision` ) → `double precision` Cosine, argument in radians  `cos(0)` → `1`  
`cosd` ( `double precision` ) → `double precision` Cosine, argument in degrees  `cosd(60)` → `0.5`  
`cot` ( `double precision` ) → `double precision` Cotangent, argument in radians  `cot(0.5)` → `1.830487721712452`  
`cotd` ( `double precision` ) → `double precision` Cotangent, argument in degrees  `cotd(45)` → `1`  
`sin` ( `double precision` ) → `double precision` Sine, argument in radians  `sin(1)` → `0.8414709848078965`  
`sind` ( `double precision` ) → `double precision` Sine, argument in degrees  `sind(30)` → `0.5`  
`tan` ( `double precision` ) → `double precision` Tangent, argument in radians  `tan(1)` → `1.5574077246549023`  
`tand` ( `double precision` ) → `double precision` Tangent, argument in degrees  `tand(45)` → `1`  
  
  


### Note

Another way to work with angles measured in degrees is to use the unit transformation functions ``radians()`` and ``degrees()`` shown earlier. However, using the degree-based trigonometric functions is preferred, as that way avoids round-off error for special cases such as `sind(30)`. 

[Table 9.8](functions-math.md#FUNCTIONS-MATH-HYP-TABLE "Table 9.8. Hyperbolic Functions") shows the available hyperbolic functions. 

**Table 9.8. Hyperbolic Functions**

Function  Description  Example(s)   
---  
`sinh` ( `double precision` ) → `double precision` Hyperbolic sine  `sinh(1)` → `1.1752011936438014`  
`cosh` ( `double precision` ) → `double precision` Hyperbolic cosine  `cosh(0)` → `1`  
`tanh` ( `double precision` ) → `double precision` Hyperbolic tangent  `tanh(1)` → `0.7615941559557649`  
`asinh` ( `double precision` ) → `double precision` Inverse hyperbolic sine  `asinh(1)` → `0.881373587019543`  
`acosh` ( `double precision` ) → `double precision` Inverse hyperbolic cosine  `acosh(1)` → `0`  
`atanh` ( `double precision` ) → `double precision` Inverse hyperbolic tangent  `atanh(0.5)` → `0.5493061443340548`  
  
  


* * *

[Prev](functions-comparison.md "9.2. Comparison Functions and Operators") | [Up](functions.md "Chapter 9. Functions and Operators")|  [Next](functions-string.md "9.4. String Functions and Operators")  
---|---|---  
9.2. Comparison Functions and Operators | [Home](index.md "PostgreSQL 17.5 Documentation")|  9.4. String Functions and Operators
