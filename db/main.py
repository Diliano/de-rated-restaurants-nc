from fastapi import FastAPI
from pydantic import BaseModel
from pg8000.native import literal
from connection import connect_to_db, close_db_connection


app = FastAPI()


@app.get("/")
def read_root():
    return {"message": "all ok"}


@app.get("/restaurants")
def read_restaurants():
    conn = connect_to_db()
    restaurants_data = conn.run("""SELECT * FROM restaurants;""")
    column_names = [c["name"] for c in conn.columns]
    formatted_restaurants_data = [dict(zip(column_names, restaurant)) for restaurant in restaurants_data]
    close_db_connection(conn)
    return {"restaurants": formatted_restaurants_data}


class NewRestaurant(BaseModel):
    restaurant_name: str
    area_id: int
    cuisine: str
    website: str

@app.post("/restaurants")
def add_new_restaurant(new_restaurant: NewRestaurant):
    conn = connect_to_db()
    insert_query = f"""
        INSERT INTO restaurants
            (restaurant_name, area_id, cuisine, website)
        VALUES
            ({literal(new_restaurant.restaurant_name)}, {literal(new_restaurant.area_id)}, {literal(new_restaurant.cuisine)}, {literal(new_restaurant.website)})
        RETURNING restaurant_id;
    """
    restaurant_id = conn.run(sql=insert_query)[0][0]
    restaurant_data = conn.run(f"""SELECT * FROM restaurants WHERE restaurant_id = {literal(restaurant_id)}""")[0]
    column_names = [c["name"] for c in conn.columns]
    formatted_restaurant_data = dict(zip(column_names, restaurant_data))
    close_db_connection(conn)
    return {"restaurant": formatted_restaurant_data}