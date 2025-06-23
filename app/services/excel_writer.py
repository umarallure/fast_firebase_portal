import pandas as pd
from io import BytesIO
from datetime import datetime
from typing import List, Dict, Any

def generate_excel(opportunities: List[Dict[str, Any]]) -> BytesIO:
    """Generate in-memory Excel file from formatted opportunities"""
    df = pd.DataFrame(opportunities)
    
    # Convert datetime objects to strings
    datetime_cols = ["created_at", "updated_at"]
    for col in datetime_cols:
        if col in df.columns:
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, 
                   sheet_name="GHL Opportunities Export",
                   index=False,
                   freeze_panes=(1, 0))
        
        worksheet = writer.sheets["GHL Opportunities Export"]
        for idx, col in enumerate(df.columns):
            max_len = max(df[col].astype(str).str.len().max(), len(col)) + 2
            worksheet.column_dimensions[chr(65+idx)].width = max_len
    
    output.seek(0)
    return output