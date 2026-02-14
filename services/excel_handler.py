from io import BytesIO
import pandas as pd

def read_excel_from_drive(file_bytes):
    df = pd.read_excel(BytesIO(file_bytes))
    return df.to_dict()
