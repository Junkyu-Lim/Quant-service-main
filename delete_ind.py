import duckdb
con = duckdb.connect('data/quant.duckdb')
con.execute("DELETE FROM indicators WHERE collected_date = '20260227'")
print('Deleted old indicators for 20260227')
con.close()
