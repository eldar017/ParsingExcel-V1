#!/bin/bash

find /opt/ExcelBackup/* -mmin +1 -exec rm {} \;
