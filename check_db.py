import duckdb
con = duckdb.connect('data/quant.duckdb')
res = con.execute("SELECT * FROM indicators WHERE 종목코드 = '005380' AND 지표구분 = 'DPS'").fetchdf()
print(res)
