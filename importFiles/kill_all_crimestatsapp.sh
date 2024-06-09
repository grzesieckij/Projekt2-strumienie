#!/bin/bash

# Get all PIDs of running CrimeStatsApp instances
pids=$(ps aux | grep 'CrimeStatsApp' | grep -v 'grep' | awk '{print $2}')

# Check if any PIDs were found
if [ -z "$pids" ]; then
  echo "No CrimeStatsApp instances found."
else
  # Loop through each PID and kill it
  for pid in $pids; do
    echo "Killing process with PID: $pid"
    kill -9 $pid
  done
  echo "All CrimeStatsApp instances killed."
fi

