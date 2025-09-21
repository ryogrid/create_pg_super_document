Appendix B. Date/Time Support  
---  
[Prev](errcodes-appendix.md "Appendix A. PostgreSQL Error Codes") | [Up](appendixes.md "Part VIII. Appendixes")| Part VIII. Appendixes| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](datetime-input-rules.md "B.1. Date/Time Input Interpretation")  
  
* * *

## Appendix B. Date/Time Support

**Table of Contents**

[B.1. Date/Time Input Interpretation](datetime-input-rules.md)
[B.2. Handling of Invalid or Ambiguous Timestamps](datetime-invalid-input.md)
[B.3. Date/Time Key Words](datetime-keywords.md)
[B.4. Date/Time Configuration Files](datetime-config-files.md)
[B.5. POSIX Time Zone Specifications](datetime-posix-timezone-specs.md)
[B.6. History of Units](datetime-units-history.md)
[B.7. Julian Dates](datetime-julian-dates.md)

PostgreSQL uses an internal heuristic parser for all date/time input support. Dates and times are input as strings, and are broken up into distinct fields with a preliminary determination of what kind of information can be in the field. Each field is interpreted and either assigned a numeric value, ignored, or rejected. The parser contains internal lookup tables for all textual fields, including months, days of the week, and time zones. 

This appendix includes information on the content of these lookup tables and describes the steps used by the parser to decode dates and times. 

* * *

[Prev](errcodes-appendix.md "Appendix A. PostgreSQL Error Codes") | [Up](appendixes.md "Part VIII. Appendixes")|  [Next](datetime-input-rules.md "B.1. Date/Time Input Interpretation")  
---|---|---  
Appendix A. PostgreSQL Error Codes | [Home](index.md "PostgreSQL 17.5 Documentation")|  B.1. Date/Time Input Interpretation
