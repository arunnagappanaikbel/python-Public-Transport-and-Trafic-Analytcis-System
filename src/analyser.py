import pandas as pd
import numpy as np

def analyze_delay_vs_congestion(df):
    grouped = df.groupby(['hour']).agg({
        'delay_minutes':'mean',
        'speed':'mean',
        'passenger_count':'sum'
    }).reset_index()
    
    return grouped