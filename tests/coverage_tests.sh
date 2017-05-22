#!/bin/bash

echo "Checking unit test code coverage"
pytest ./dashan-etl --cov ./dashan-etl/etl
