import pandas as pd
import numpy as np

def transform_trafic_data(traffic_josn):
    rows = []
    for road in traffic_josn['roads']:
        name = road['name']
        congestion = road['traffic']['congestion_level']
        for entry in road['traffic']['timestamps']:
            rows.append({
                'road':name,
                'congestion_level':congestion,
                'time':entry['time'],
                'speed':entry['speed']
            })    
    return pd.DataFrame(rows)

def enrich_transport_data(df):
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['congestion_score'] = np.where(df['delay_minutes'] > 5, 3,
                               np.where(df['delay_minutes'] > 0, 2, 1))
    
    return df

def merge_data(trafric_df, transport_df):
    trafric_df['hour'] = pd.to_datetime(trafric_df['time'],format='%H:%M').dt.hour
    transport_df['hour'] = transport_df['timestamp'].dt.hour
    
    merged = pd.merge(transport_df, trafric_df, on='hour', how='inner')
    return merged
    
    