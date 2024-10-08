from fastapi import FastAPI
from connection import connect_to_db, close_db_connection


app = FastAPI()


@app.get("/")
def get_root():
    return {"message": "all ok"}


@app.get("/restaurants")
def get_restaurants():
    conn = connect_to_db()
    restaurants_data = conn.run("""SELECT * FROM restaurants;""")
    column_names = [c["name"] for c in conn.columns]
    formatted_restaurants_data = [dict(zip(column_names, restaurant)) for restaurant in restaurants_data]
    close_db_connection(conn)
    return {"restaurants": formatted_restaurants_data}