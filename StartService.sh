#!/bin/bash

PYTHON_EXE=/home/omloo/software/micromamba/envs/rest/bin/python
SCRIPT_PATH=/home/omloo/projects/real_estate/
LOG_PATH=/var/log/omloo/

${PYTHON_EXE} -u ${SCRIPT_PATH}/rest-runner.py #2>>${LOG_PATH}/rest-runner.err 1>>${LOG_PATH}/rest-runner.log
