from flask import Flask, request, jsonify  
from datetime import datetime
from data.market_data_collector import MarketDataCollector
from dotenv import load_dotenv
import os

load_dotenv()
app = Flask(__name__)

# Initialize MarketDataCollector with no API key for now
collector = MarketDataCollector(api_key=None)

# Load allowed exchanges and timeframes from environment variables
allowed_exchanges = os.getenv('ALLOWED_EXCHANGES')
allowed_timeframes = os.getenv('ALLOWED_TIMEFRAMES')

@app.route('/allowed-params', methods=['GET'])
def allowed_params():
    # Endpoint to return allowed exchanges and timeframes
    return {
        'timeframes': allowed_timeframes, 'exchanges': allowed_exchanges
    }

@app.route('/get-candle', methods=['GET'])
def get_candle():
    """
    Endpoint to fetch candlestick data for a cryptocurrency pair from a specific exchange.
    """
    # Get query parameters with default values
    exchange = request.args.get('exchange', 'binance')
    symbol = request.args.get('symbol', 'BTC/USDT')
    timeframe = request.args.get('timeframe', '1d')
    
    # Check if the symbol and timeframe are valid using the collector
    check_params = collector.crypto.check_symbol_and_timeframe(exchange_name=exchange, symbol=symbol, timeframe=timeframe)
    if check_params: return check_params

    # Validate if the exchange is allowed
    if exchange not in allowed_exchanges:
        return jsonify({
            "status": "error", 
            "message": f"Exchange '{exchange}' is not allowed. Allowed exchanges: {allowed_exchanges}"
        }), 400

    # Validate if the timeframe is allowed
    if timeframe not in allowed_timeframes:
        return jsonify({
            "status": "error", 
            "message": f"Timeframe '{timeframe}' is not allowed. Allowed timeframes: {allowed_timeframes}"
        }), 400
    
    # Get additional query parameters
    limit = int(request.args.get('limit', 1))  # Default limit is 1
    since_str = request.args.get('since', None)  # Optional 'since' parameter
    until_str = request.args.get('until', None)  # Optional 'until' parameter
    
    try:
        if since_str and until_str:
            # Parse 'since' and 'until' parameters if provided
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
            
            # Fetch candlestick data by date range
            df = collector.crypto.fetch_by_date(
                exchange_name=exchange,
                symbol=symbol,
                timeframe=timeframe,
                since=since,
                until=until
            )
        else:
            # Fetch candlestick data by limit if no date range is provided
            df = collector.crypto.fetch_by_limit(
                    exchange_name=exchange,
                    symbol=symbol,
                    limit=limit,
                    timeframe=timeframe
                )
            
        # Convert the data to a list of dictionaries
        candles = df.to_dict("records")
        return jsonify({
            "status": "success",
            "meta":{
                "timeframe": timeframe,
                "exchange": exchange,
                "pair": symbol,
                "from": since,
                "to": until,
                "count": len(candles),  
            },
            "data": candles,
            
        })
    except Exception as e:
        # Handle any unexpected errors
        return jsonify({"status": "error", "message": str(e)}), 500
    
if __name__ == '__main__':
    # Run the Flask app on all network interfaces so it can be accessed externally
    app.run(host='0.0.0.0', port=os.getenv('PORT'))