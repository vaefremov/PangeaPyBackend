#!/bin/bash -x
echo 'ReView server starting...'
. ./venv/bin/activate
uvicorn reviewp4:app --reload

