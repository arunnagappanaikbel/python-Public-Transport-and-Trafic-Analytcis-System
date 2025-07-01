import os

def save_dataframe(df, path, filename):
    os.makedirs(path, exist_ok=True)
    df.to_csv(os.path.join(path,filename), index=False)