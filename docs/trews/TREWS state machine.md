TREWS-based State Machine v2.0 2017-09-29
===================

| State        | Definition    | Comment |
| ------------- |:--------------|:--------|
| 0 | no conditions present |  OK  |
| 1 | severe sepsis not present; note not present; 1 sirs present |  USE?  |
| 2 | severe sepsis not present; note not present; 1 orgdf present |  USE?  |
| 3 | severe sepsis not present; note not present; 2 sirs present |  USE?  |
| 4 | severe sepsis not present; note not present; 1 sirs & 1 orgdf present |  USE?  |
| 5 | severe sepsis not present; note present & 1 sirs present |  USE?  |
| 6 | severe sepsis not present; note present & 1 orgdf present |  USE?  |
| 7 | severe sepsis not present; note present & 2 sirs present |  USE?  |
| 8 | severe sepsis not present; note present, 1 sirs & 1 orgdf present |  USE?  |
| 10 | severe sepsis not present; (2 sirs or trews) & 1 orgdf present, no note |  CHG  |
| 12 | severe sepsis not present; (2 sirs or trews) & 1 orgdf present, no infection marked by provider |  CHG  |
| 13 | severe sepsis not present; (2 sirs or trews) & 1 orgdf present, note present (implies sirs&orgdf and note not within 6 hrs) |  USE?  |
| 20 | CMS-based severe sepsis present |  OK  |
| 21 | CMS-based severe sepsis present; 3hr bundle completed |  OK  |
| 22 | CMS-based severe sepsis present; 3hr bundle expired |  OK  |
| 23 | CMS-based severe sepsis present; 6hr bundle completed |  OK  |
| 24 | CMS-based severe sepsis present; 6hr bundle expired |  OK  |
| 30 | CMS-based severe sepsis & septic shock present |  OK  |
| 31 | CMS-based severe sepsis & septic shock present; severe sepsis sep 3hr bundle completed |  OK  |
| 32 | CMS-based severe sepsis & septic shock present; severe sepsis 3hr bundle expired |  OK  |
| 33 | CMS-based severe sepsis & septic shock present; severe sepsis sep 6hr bundle completed |  OK  |
| 34 | CMS-based severe sepsis & septic shock present; severe sepsis 6hr bundle expired |  OK  |
| 35 | CMS-based severe sepsis & septic shock present; septic shock 6hr bundle completed |  OK  |
| 36 | CMS-based severe sepsis & septic shock present; septic shock 6hr bundle expired |  OK  |
| 40 | TREWS-based severe sepsis present |  NEW  |
| 41 | TREWS-based severe sepsis present; 3hr bundle completed |  NEW  |
| 42 | TREWS-based severe sepsis present; 3hr bundle expired |  NEW  |
| 43 | TREWS-based severe sepsis present; 6hr bundle completed |  NEW  |
| 44 | TREWS-based severe sepsis present; 6hr bundle expired |  NEW  |
| 50 | TREWS-based severe sepsis & septic shock present |  NEW  |
| 51 | TREWS-based severe sepsis & septic shock present; severe sepsis sep 3hr bundle completed |  NEW  |
| 52 | TREWS-based severe sepsis & septic shock present; severe sepsis 3hr bundle expired |  NEW  |
| 53 | TREWS-based severe sepsis & septic shock present; severe sepsis sep 6hr bundle completed |  NEW  |
| 54 | TREWS-based severe sepsis & septic shock present; severe sepsis 6hr bundle expired |  NEW  |
| 55 | TREWS-based severe sepsis & septic shock present; septic shock 6hr bundle completed |  NEW  |
| 56 | TREWS-based severe sepsis & septic shock present; septic shock 6hr bundle expired |  NEW  |
