#!/bin/bash
echo "Running tests"
export trews_open_access=True
python dashan-universe-pr/test/test_server_alive.py
