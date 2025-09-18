# j2date

## Location
src/interfaces/ecpg/pgtypeslib/dt_common.c: 606 - 634

## Overview
Converts a Julian day number back to calendar date components (year, month, day), serving as the inverse function to date2j().

## Definition


## Detailed Description
j2date implements the inverse of the date2j() function, converting a Julian day number back to its corresponding calendar date components. The algorithm uses a series of mathematical operations involving division, modulo arithmetic, and carefully chosen constants to decompose the Julian day number into year, month, and day values.

The function employs unsigned integer arithmetic to handle the complex calculations required for the reverse Julian-to-calendar conversion. It accounts for leap years, varying month lengths, and the Gregorian calendar system through a sophisticated algorithm that uses quad-year cycles and other astronomical constants. The implementation handles the same range as date2j(), working correctly for Julian day numbers corresponding to dates from Nov 24, -4713 onwards.

## Parameters / Member Variables
- : The Julian day number to convert to calendar date
- : Output parameter that receives the calendar year
- : Output parameter that receives the month (1-12)
- : Output parameter that receives the day of the month (1-31)

## Dependencies
- Functions called/Symbols referenced:
  - MONTHS_PER_YEAR (constant defining 12 months per year)
- Called from (representative examples):
  - [date_out](../d/date_out.md) (date output formatting)
  - [DecodeDateTime](../D/DecodeDateTime.md) (date/time parsing operations)
  - [timestamp2tm](../t/timestamp2tm.md) (timestamp to tm structure conversion)
  - [extract_date](../e/extract_date.md) (date component extraction)
  - [isoweek2date](../i/isoweek2date.md) (ISO week to date conversion)
  - ValidateDate (date validation)

## Notes and Other Information
- Function returns void and modifies the output parameters passed by reference
- Uses unsigned integer arithmetic for intermediate calculations to avoid overflow issues
- The algorithm involves several magic numbers (32044, 146097, 1461, etc.) derived from astronomical and calendar calculations
- Central to PostgreSQL's date output and conversion operations
- Works as the exact inverse of date2j() - applying both functions in sequence returns the original values
- Essential for displaying dates to users and converting internal Julian representations back to human-readable format
- The complex arithmetic handles leap year cycles, century adjustments, and month/day calculations in a mathematically efficient manner