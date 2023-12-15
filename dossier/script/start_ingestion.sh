#!/usr/bin/sh
set -e
script_dirpath=$(dirname $0)
cd $script_dirpath/..
make install
source script/app_profile.sh

PYTHON_PROG_MAIN="$UNXPACKAGE/pipeline/app_pipeline.py"
python $PYTHON_PROG_MAIN ingestion
