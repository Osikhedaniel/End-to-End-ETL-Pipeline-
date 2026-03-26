import psycopg2

conn = psycopg2.connect(
    host="dpg-d72idu19fqoc73aa1csg-a.frankfurt-postgres.render.com",
    database="zion_etl_2_pipeline_2",
    user="zion_etl_2_pipeline_2_user",
    password="1RxPgBimWE6j8I0devUchEaioY7TwRp6",
    sslmode="require"
)

print("CONNECTED SUCCESSFULLY")
conn.close()