#!/bin/bash

NODE=$(hostname)

echo "Hello world $(hostname)" > ${HOME}/cass-startup-${NODE}.log 2>&1 &