<p align="center">
  <img src="https://cloud.githubusercontent.com/assets/7926463/24318897/34c9fefc-10e4-11e7-96c2-1433d7cbf168.png" width=72 height=72>

  <h3 align="center">Dashan ETL</h3>

  <p align="center">
    Extract Transform Load engine from all sources to all sinks.
  </p>
</p>

<br>

---


# Getting Started

    git clone https://github.com/dashan-emr/dashan-etl.git
    cd dashan-etl
    pip install -r requirements.txt
    pip install -e .


# Project layout

```
etl/
├── archive/
│   ├── engine.py
│   ├── extractor.py
│   └── utils/
├── core/
│   ├── engine.py
│   └── task.py
├── offline/
│   ├── engine.py
│   ├── extractor.py
│   ├── ...
│   └── utils/
├── online/
│   ├── engine.py
│   ├── extractor.py
│   └── utils/
└── transforms/
    ├── primitives/
    │   ├── df/
    │   ├── row/
    │   ├── native/
    │   └── tbl/
    └── pipelines/

```

# Definitions

## Core
_Scheduler implementation for an ETL pipeline_

- Engine Class
    - Maintains a queue of work and executes that queue
- Task
    - The representation of work

## Transforms
_All functionality related to transforming data from its raw format to final output_

- Primitive, stateless transform functions (defined by data object type)
- Pipelines - configuration files defining transformation pipelines

## Online

- JHAPI
&nbsp;&nbsp;&nbsp;:arrow_right:&nbsp;&nbsp;&nbsp;&nbsp;
{prod,dev} database

## Offline

- Clarity
&nbsp;&nbsp;&nbsp;:arrow_right:&nbsp;&nbsp;&nbsp;&nbsp;
data-warehouse database

## Archive

- Prod
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;:arrow_right:&nbsp;&nbsp;&nbsp;&nbsp;
data-warehouse database


---
