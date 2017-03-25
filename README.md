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
    pip3 install virtualenvwrapper
    mkvirtualenv dashan-etl --python=python3.6
    pip3 install -r requirements.txt
    pip3 install -e .


# Project layout

```
etl/
├── clarity2dw/
├── core/
│   ├── config.py
│   ├── engine.py
│   ├── exceptions.py
│   └── task.py
├── epic2op/
├── mappings/
├── op2dw/
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
- Task Class
    - The representation of work
- Config
    - Shared constants and variables across entire project
- Exceptions
    - Custom exceptions

## Transforms
_All functionality related to transforming data from its raw format to final output_

- Primitive, stateless transform functions (defined by data object type)
- Pipelines - configuration files defining transformation pipelines

## epic2op

- JHAPI
&nbsp;&nbsp;&nbsp;:arrow_right:&nbsp;&nbsp;&nbsp;&nbsp;
{prod,dev} operations database

## clarity2dw

- Clarity
&nbsp;&nbsp;&nbsp;:arrow_right:&nbsp;&nbsp;&nbsp;&nbsp;
data-warehouse database

## op2dw

- Prod
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;:arrow_right:&nbsp;&nbsp;&nbsp;&nbsp;
data-warehouse database


---
