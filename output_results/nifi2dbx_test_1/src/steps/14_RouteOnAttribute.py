# RouteOnAttribute → DataFrame filter() operations
# Routes data based on attribute values

df_route1 = df.filter(col('{attribute}') == '{value}')