#!/bin/bash

echo "Checking etl unit test code coverage"
pytest ./dashan-universe/etl --cov ./dashan-universe/etl
