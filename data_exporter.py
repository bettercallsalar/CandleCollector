import os
import pandas as pd
import logging

# Set up logging for the module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class DataExporter:
    @staticmethod
    def save_to_csv(df, name, timeframe, exchange):
        folder = os.path.join("data", exchange, name)
        os.makedirs(folder, exist_ok=True)

        filename = f"{timeframe.upper()}.csv"
        path = os.path.join(folder, filename)

        # If file exists, load and merge to avoid duplicate timestamps
        if os.path.exists(path):
            existing_df = pd.read_csv(path, parse_dates=['timestamp'])
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df.drop_duplicates(subset='timestamp', keep='last', inplace=True)
            combined_df.sort_values(by='timestamp', inplace=True)
        else:
            combined_df = df.copy()
            combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])

        # Save merged dataframe
        combined_df.to_csv(path, index=False)
        logger.info(f"[✓] Data saved to {path} ({len(combined_df)} total rows)")
