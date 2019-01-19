#!/usr/bin/env bash

outputFolder=${1:-data}

# fail on first error
set -e
# prepare
mkdir -p ${outputFolder}

# download one month data from july 95
if [[ ! -f "${outputFolder}/access_log_Jul95" ]]; then
    echo "Downloading HTTP log data from July '95"
    curl -s ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz | gunzip - > "${outputFolder}/access_log_Jul95"
else
    echo "HTTP log data from july does already exist"
fi

if [[ ! -f "${outputFolder}/access_log_Aug95" ]]; then
    echo "Downloading HTTP log data from one week in August '95"
    curl -s ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz | gunzip - > "${outputFolder}/access_log_Aug95"
else
    echo "HTTP log data from august does already exist"
fi

echo "Script finished"
exit 0
