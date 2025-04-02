from flask import Flask, request, jsonify
from datetime import datetime
from data.market_data_collector import MarketDataCollector
from dotenv import load_dotenv
import os

load_dotenv()
app = Flask(__name__)
collector = MarketDataCollector(api_key=None) # api key is for forex data and for now is None

allowed_exchanges = os.getenv('ALLOWED_EXCHANGES')
allowed_timeframes = os.getenv('ALLOWED_TIMEFRAMES')

@app.route('/allowed-params', methods=['GET'])
def allowed_params():
    return {
        'timeframes': allowed_timeframes, 'exchanges': allowed_exchanges
    }

@app.route('/get-candle', methods=['GET'])
def get_candle():
    """
    Endpoint to fetch candlestick data for a cryptocurrency pair from a specific exchange.
    """
    exchange = request.args.get('exchange', 'binance')
    symbol = request.args.get('symbol', 'BTC/USDT')
    timeframe = request.args.get('timeframe', '1d')
    
     # Validate allowed exchange and timeframe
    if exchange not in allowed_exchanges:
        return jsonify({
            "status": "error", 
            "message": f"Exchange '{exchange}' is not allowed. Allowed exchanges: {allowed_exchanges}"
        }), 400
    if timeframe not in allowed_timeframes:
        return jsonify({
            "status": "error", 
            "message": f"Timeframe '{timeframe}' is not allowed. Allowed timeframes: {allowed_timeframes}"
        }), 400
    
    limit = int(request.args.get('limit', 1))
    since_str = request.args.get('since', None)
    until_str = request.args.get('until', None)
    
    try:
        if since_str and until_str:
            try:
                since = datetime.fromisoformat(since_str)
            except:
                return jsonify({
                    "status": "error",
                    "message": f"Invalid 'since' date format. Expected ISO format (YYYY-MM-DDTHH:MM:SS)."
                }), 400
            try:
                until = datetime.fromisoformat(until_str)
            except:
                return jsonify({
                    "status": "error",
                    "message": f"Invalid 'until' date format. Expected ISO format (YYYY-MM-DDTHH:MM:SS)."
                }),  400
                
            df = collector.crypto.fetch_by_date(
                exchange_name=exchange,
                symbol=symbol,
                timeframe=timeframe,
                since=since,
                until=until
            )
        else:
            df = collector.crypto.fetch_by_limit(
                    exchange_name=exchange,
                    symbol=symbol,
                    limit=limit,
                    timeframe=timeframe
                )
            
        candles = df.to_dict("records")
        return jsonify({
            "status": "success",
            "meta":{
                "timeframe": timeframe,
                "exchange": exchange,
                "pair": symbol,
                "from": since,
                "to": until,
                "count": len(candles)
            },
            "data": candles,
            
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    
if __name__ == '__main__':
    # Run on all network interfaces so it can be accessed externally
    app.run(host='0.0.0.0', port=5000)