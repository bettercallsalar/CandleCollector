#!/bin/bash
# run_multiple.sh
cd ..
export EXCHANGE="binance"
export SYMBOL="BTC/USDT"
export TIMEFRAMES="1w,3d,1d,12h,8h,6h,4h,2h,1h,30m,15m,5m,3m,1m"
export MONGODB_URI="mongodb://admin:bgKdk,Qcj'Zp)~X6maDwfW@localhost:27017/"
python3 main.py &
export EXCHANGE="binance"
export SYMBOL="ETH/USDT"
export TIMEFRAMES="1w,3d,1d,12h,8h,6h,4h,2h,1h,30m,15m,5m,3m,1m"
export MONGODB_URI="mongodb://admin:bgKdk,Qcj'Zp)~X6maDwfW@localhost:27017/"
python3 main.py &
wait
