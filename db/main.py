from fastapi import FastAPI, Response
from connection import connect_to_db, close_db_connection
from pg8000.native import literal
from pydantic import BaseModel
from typing import Optional


app = FastAPI()


@app.get("/api")
def read_root():
    return {"message": "all ok"}


@app.get("/api/restaurants")
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

@app.post("/api/restaurants")
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


@app.delete("/api/restaurants/{restaurant_id}", status_code=204)
def delete_restaurant(restaurant_id: int):
    conn = connect_to_db()
    conn.run(f"""DELETE FROM restaurants WHERE restaurant_id = {literal(restaurant_id)};""")
    close_db_connection(conn)


class UpdatedAreaCode(BaseModel):
    area_id: Optional[int] = None

@app.patch("/api/restaurants/{restaurant_id}")
def update_area_id(restaurant_id: int, updated_area_id: UpdatedAreaCode, response: Response):
    if not dict(updated_area_id)["area_id"]:
        response.status_code = 400
        return {"message": "empty request body"}

    conn = connect_to_db()
    conn.run(f"""UPDATE restaurants SET area_id = {literal(updated_area_id.area_id)} WHERE restaurant_id = {literal(restaurant_id)};""")
    restaurant_data = conn.run(f"""SELECT * FROM restaurants WHERE restaurant_id = {literal(restaurant_id)}""")[0]
    column_names = [c["name"] for c in conn.columns]
    formatted_restaurant_data = dict(zip(column_names, restaurant_data))
    close_db_connection(conn)
    return {"restaurant": formatted_restaurant_data}

