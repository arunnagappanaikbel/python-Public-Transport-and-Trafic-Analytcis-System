import pandas as pd
import os
import logging
import json

def load_json(path):
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except Exception as e:
        raise RuntimeError(f"Error loading JSON from {path}: {e}")

def load_csv(path):
    try:
        return pd.read_csv(path)
    except Exception as e:
        raise RuntimeError(f"Error loading CSV from {path}: {e}")