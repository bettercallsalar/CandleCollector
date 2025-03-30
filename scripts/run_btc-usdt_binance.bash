#!/bin/bash

# Run AlgoTrade updater for BTC/USDT on Binance
export EXCHANGE="binance"
export SYMBOL="BTC/USDT"
export TIMEFRAMES="1w,3d,1d,12h,8h,6h,4h,2h,1h,30m,15m,5m,3m,1m"
export MONGODB_URI="mongodb://admin:bgKdk,Qcj'Zp)~X6maDwfW@localhost:9631/"
echo "Starting updater for $EXCHANGE $SYMBOL..."
python3 main.py &

wait