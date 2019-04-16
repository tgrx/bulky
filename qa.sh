#!/usr/bin/env bash

# ---------------------------------------------------------
abort()
{
    if test -n "$1"; then
        echo >&2 -e "\n\nFAILED: $1\n\n"
    fi
    exit 1
}

trap 'abort' 0

set -e
set -o pipefail

# ---------------------------------------------------------

# format code

black . || echo "BLACK is not installed, skipping code formatting"

# check static typing

pipenv run mypy \
    bulky/ tests/ \
|| abort 'STATIC TYPING CHECK'

# run tests & prepare coverage report

pipenv run coverage run --source bulky/ setup.py test \
|| abort 'TESTS'

# check that coverage is > 90%

pipenv run coverage report

(( $(pipenv run coverage report | grep 'TOTAL' | awk '{print $4}' | sed -e 's/%//g') > 90 )) \
|| abort 'COVERAGE < 90%'


# ---------------------------------------------------------
trap : 0
# ---------------------------------------------------------
