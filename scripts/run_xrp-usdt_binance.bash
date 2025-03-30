#!/bin/bash
# run_xrp-usdt_binance.sh

# Change directory to the project root where main.py is located.
cd ..

# Set environment variables for this specific configuration.
export EXCHANGE="binance"
export SYMBOL="XRP/USDT"
export TIMEFRAMES="1w,3d,1d,12h,8h,6h,4h,2h,1h,30m,15m,5m,3m,1m"
export MONGODB_URI="mongodb://admin:bgKdk,Qcj'Zp)~X6maDwfW@localhost:9631/"

# Option 1: Run interactively (logs appear in the terminal)
#python3 main.py

# Option 2: Run in the background and redirect output to a log file
python3 main.py > xrp_usdt_binance.log 2>&1 &
