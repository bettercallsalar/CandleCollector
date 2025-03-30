#!/bin/bash
# run_multiple.sh
# This script runs the AlgoTrade updater for multiple configurations.
# Each configuration is defined as "exchange symbol" (e.g., "binance BTC/USDT").
# Make sure this script is located in a folder (e.g., "scripts") that is a subdirectory
# of your project root (where main.py is located).

# Change directory to the project root where main.py exists.
cd ..

# Define an array of configurations (each entry: "exchange symbol").
configs=(
  "binance BTC/USDT"
)

# Define default values (these can also be set in your .env file if desired).
DEFAULT_TIMEFRAMES="1w,3d,1d,12h,8h,6h,4h,2h,1h,30m,15m,5m,3m,1m"
DEFAULT_MONGO_URI="mongodb://admin:bgKdk,Qcj'Zp)~X6maDwfW@localhost:9631/"

# Loop through each configuration and start the updater.
for config in "${configs[@]}"; do
    # Extract exchange and symbol (split on space)
    exchange=$(echo $config | awk '{print $1}')
    symbol=$(echo $config | awk '{print $2}')
    
    echo "Starting updater for $exchange $symbol..."
    
    # Set environment variables for main.py to read.
    export EXCHANGE="$exchange"
    export SYMBOL="$symbol"
    export TIMEFRAMES="$DEFAULT_TIMEFRAMES"
    export MONGODB_URI="$DEFAULT_MONGO_URI"
    
    # Run main.py in the background.
    python3 main.py &
done

# Wait for all background jobs to finish (this may run indefinitely if the updaters are long-running)
wait
