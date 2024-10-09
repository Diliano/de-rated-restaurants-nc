from fastapi import FastAPI, Response
from connection import connect_to_db, close_db_connection
from pg8000.native import literal
from pydantic import BaseModel
from typing import Optional


app = FastAPI()


"""
Error handling considerations for GET "/api":
- path is incorrect; 404 default handled by FastAPI
- method does not exist; 405 default handled by FastAPI
"""
@app.get("/api")
def read_root():
    return {"message": "all ok"}


@app.get("/api/areas/{area_id}/restaurants")
def read_area_restaurants(area_id: int):
    conn = connect_to_db()
    select_query = f"""
        SELECT areas.*, COUNT(restaurant_name) as total_restaurants, ARRAY_AGG(restaurant_name) as restaurants
        FROM areas
        JOIN restaurants ON areas.area_id = restaurants.area_id
        WHERE areas.area_id = {literal(area_id)}
        GROUP BY areas.area_id;
    """
    area_restaurants_data = conn.run(sql=select_query)[0]
    column_names = [c["name"] for c in conn.columns]
    formatted_data = dict(zip(column_names, area_restaurants_data))
    close_db_connection(conn)
    return {"area": formatted_data}


@app.get("/api/restaurants")
def read_restaurants():
    conn = connect_to_db()
    restaurants_data = conn.run("""
        SELECT restaurants.*, ROUND(AVG(rating), 1) as average_rating
        FROM restaurants
        JOIN ratings ON restaurants.restaurant_id = ratings.restaurant_id
        GROUP BY restaurants.restaurant_id
        ORDER BY restaurants.restaurant_id;
    """)
    column_names = [c["name"] for c in conn.columns]
    formatted_restaurants_data = [dict(zip(column_names, restaurant)) for restaurant in restaurants_data]
    close_db_connection(conn)
    return {"restaurants": formatted_restaurants_data}


class NewRestaurant(BaseModel):
    restaurant_name: str
    area_id: int
    cuisine: str
    website: str

@app.post("/api/restaurants", status_code=201)
def add_new_restaurant(new_restaurant: NewRestaurant):
    conn = connect_to_db()
    insert_query = f"""
        INSERT INTO restaurants
            (restaurant_name, area_id, cuisine, website)
        VALUES
            ({literal(new_restaurant.restaurant_name)}, {literal(new_restaurant.area_id)}, {literal(new_restaurant.cuisine)}, {literal(new_restaurant.website)})
        RETURNING *;
    """
    restaurant_data = conn.run(sql=insert_query)[0]
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
    restaurant_data = conn.run(f"""UPDATE restaurants SET area_id = {literal(updated_area_id.area_id)} WHERE restaurant_id = {literal(restaurant_id)} RETURNING *;""")[0]
    column_names = [c["name"] for c in conn.columns]
    formatted_restaurant_data = dict(zip(column_names, restaurant_data))
    close_db_connection(conn)
    return {"restaurant": formatted_restaurant_data}

