#!/bin/bash
if [[ ! -v CONTAINER_COMMAND ]]; then
    dbt clean && dbt snapshot && dbt run && dbt test
elif [[ -z "$CONTAINER_COMMAND" ]]; then
    dbt clean && dbt snapshot && dbt run && dbt test
else
    dbt clean && dbt parse && eval ${CONTAINER_COMMAND}
fi