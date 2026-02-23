#!/bin/bash

echo "Starting jupyter lab..."
jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --ServerApp.token=''
