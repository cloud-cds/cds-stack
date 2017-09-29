TREWS-based State Machine v2.0 2017-09-29
===================

| State        | Definition    | Comment |
| ------------- |:--------------|:--------|
| 0 | no conditions present |  v1  |
| 1 | severe sepsis not present; note not present; 1 sirs present |  UNUSE@OL  |
| 2 | severe sepsis not present; note not present; 1 orgdf present |  UNUSE@OL  |
| 3 | severe sepsis not present; note not present; 2 sirs present |  UNUSE@OL  |
| 4 | severe sepsis not present; note not present; 1 sirs & 1 orgdf present |  UNUSE@OL  |
| 5 | severe sepsis not present; note present & 1 sirs present |  UNUSE@OL  |
| 6 | severe sepsis not present; note present & 1 orgdf present |  UNUSE@OL  |
| 7 | severe sepsis not present; note present & 2 sirs present |  UNUSE@OL  |
| 8 | severe sepsis not present; note present, 1 sirs & 1 orgdf present |  UNUSE@OL  |
| 10 | severe sepsis not present; 2 sirs & 1 orgdf present, no note |  v1  |
| 11 | severe sepsis not present; trews & 1 orgdf present, no note |  NEW  |
| 12 | severe sepsis not present; 2 sirs & 1 orgdf present, no infection marked by provider |  v1  |
| 13 | severe sepsis not present; 2 sirs & 1 orgdf present, note present (implies sirs&orgdf and note not within 6 hrs) |  UNUSE@OL  |
| 14 | severe sepsis not present; trews & 1 orgdf present, no infection marked by provider |  NEW  |
| 15 | severe sepsis not present; trews & 1 orgdf present, note present (implies trews&orgdf and note not within 6 hrs) |  NEW & UNUSE@OL |
| 20 | CMS-based severe sepsis present |  v1  |
| 21 | CMS-based severe sepsis present; 3hr bundle completed |  v1  |
| 22 | CMS-based severe sepsis present; 3hr bundle expired |  v1  |
| 23 | CMS-based severe sepsis present; 6hr bundle completed |  v1  |
| 24 | CMS-based severe sepsis present; 6hr bundle expired |  v1  |
| 25 | TREWS-based severe sepsis present |  NEW  |
| 26 | TREWS-based severe sepsis present; 3hr bundle completed |  NEW  |
| 27 | TREWS-based severe sepsis present; 3hr bundle expired |  NEW  |
| 28 | TREWS-based severe sepsis present; 6hr bundle completed |  NEW  |
| 29 | TREWS-based severe sepsis present; 6hr bundle expired |  NEW  |
| 30 | CMS-based severe sepsis & septic shock present |  v1  |
| 31 | CMS-based severe sepsis & septic shock present; severe sepsis sep 3hr bundle completed |  v1  |
| 32 | CMS-based severe sepsis & septic shock present; severe sepsis 3hr bundle expired |  v1  |
| 33 | CMS-based severe sepsis & septic shock present; severe sepsis sep 6hr bundle completed |  v1  |
| 34 | CMS-based severe sepsis & septic shock present; severe sepsis 6hr bundle expired |  v1  |
| 35 | CMS-based severe sepsis & septic shock present; septic shock 6hr bundle completed |  v1  |
| 36 | CMS-based severe sepsis & septic shock present; septic shock 6hr bundle expired |  v1  |
| 40 | TREWS-based severe sepsis & septic shock present |  NEW  |
| 41 | TREWS-based severe sepsis & septic shock present; severe sepsis sep 3hr bundle completed |  NEW  |
| 42 | TREWS-based severe sepsis & septic shock present; severe sepsis 3hr bundle expired |  NEW  |
| 43 | TREWS-based severe sepsis & septic shock present; severe sepsis sep 6hr bundle completed |  NEW  |
| 44 | TREWS-based severe sepsis & septic shock present; severe sepsis 6hr bundle expired |  NEW  |
| 45 | TREWS-based severe sepsis & septic shock present; septic shock 6hr bundle completed |  NEW  |
| 46 | TREWS-based severe sepsis & septic shock present; septic shock 6hr bundle expired |  NEW  |
