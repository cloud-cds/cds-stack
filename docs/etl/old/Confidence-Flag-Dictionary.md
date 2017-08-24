| Code   | Description           |
| -------|:-------------:|
| 0      | Null |
| 1      | No transform, directly extracted from source |
| 2      | Unit transformed  |
| 4      | Value transformed      |
| 8     | filled in |
| 16     | filled in by population mean |
The Rules to generate confidence flags for derived features:
the confidence flag for derive features are the union of codes of the features it depends on.
For example, feature A = B + C. If B and C have confidence flag 2 and 4 respectively, then A's confidence tag is 6;

When data values are loaded in CDM, their confidence values are 1 (0b1) no transform, 2 (0b10) unit transformed, or 4 (0b100) value transformed.

then, after running lastest_value_windoe fill-in, all filled in values have fill-in flag to be on 8 (0x1000)

If the value is filled-in by lastest_value, the confidence value is equal to 8 logical_or the confidence of the latest value, so confidence value 9 (0b1001) means it's a filled-in value which use the latest value (no transform); while, confidence value 10 (0b1010) means it's a filled-in value which use the latest value (unit transformed)

else, if the value is filled-in by popmean, then the confidence value is 8 | 16 = 24 (0b11000)

For derive feature, the confidence value is equal the logical or of all confidence values of the input features.
