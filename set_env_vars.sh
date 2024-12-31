#!/bin/bash

echo "Running set_env_vars.sh..."

if [ -f config.json ]; then
    echo "Found config.json."
else
    echo "Did not find config.json."
fi

function_name=$(jq -r .function_name config.json)

if [ -z "$function_name" ]; then
    echo "Function name is empty."
else
    echo "Function name: $function_name"
fi

echo "FUNCTION_NAME=$function_name" >> "$GITHUB_OUTPUT"
