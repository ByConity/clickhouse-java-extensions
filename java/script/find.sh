#!/bin/bash

jars=( $(find $1 -name "*.jar") )

for jar in ${jars[@]}
do
    if jar tf ${jar} | grep 'org.apache.log4j' > /dev/null 2>&1; then
        echo "FOUND: ${jar}"
    fi
done
