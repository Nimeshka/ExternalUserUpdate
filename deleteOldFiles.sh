#!/usr/bin/env bash

find /opt/ExtUserUpdate/ -type f -mtime +3 -execdir rm -- '{}' \;