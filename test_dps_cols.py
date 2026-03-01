import urllib.request
import pandas as pd
from quant_collector_enhanced import parse_period

url = 'https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A005380&stkGb=701'
html = urllib.request.urlopen(url).read().decode('utf-8', 'ignore')
tables = pd.read_html(html)

for t in tables:
    if len(t) > 2 and len(t.columns) > 2:
        col1_list = [str(x) for x in t.iloc[:, 0].values]
        if any('배당금' in c or 'DPS' in c for c in col1_list):
            dps_idx = t.iloc[:, 0].str.contains('배당금|DPS', na=False, regex=True).idxmax()
            row_data = t.iloc[dps_idx]
            for col_name, val in row_data.items():
                if col_name == t.columns[0]: continue
                col_str = str(col_name)
                print(f"Col: {col_str}, Annual: {'Annual' in col_str}, E: {'(E)' in col_str or '(P)' in col_str}")
            break
