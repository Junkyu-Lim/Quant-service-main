import json
from quant_collector_enhanced import fetch_indicators

ticker = '005380'
indicators = fetch_indicators(ticker)
dps_rows = [r for r in indicators if r.get('지표구분') == 'DPS' and r.get('계정') == '주당배당금']
for row in dps_rows:
    row['기준일'] = str(row['기준일'])
print(json.dumps(dps_rows, ensure_ascii=False, indent=2))
