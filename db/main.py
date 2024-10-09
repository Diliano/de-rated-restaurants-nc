from fastapi import FastAPI, Response, Request, HTTPException
from connection import connect_to_db, close_db_connection
from pg8000.native import literal, DatabaseError
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


"""
Error handling considerations for GET "/api/areas/:area_id/restaurants":
- path is incorrect; 404 default handled by FastAPI
- method does not exist; 405 default handled by FastAPI
- parameter is wrong type; 422 default handled by FastAPI
- parameter does not exist; custom 404 implemented
- server error; custom 500 implemented
"""
@app.get("/api/areas/{area_id}/restaurants")
def read_area_restaurants(area_id: int):
    conn = None
    try:
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
        return {"area": formatted_data}
    except IndexError:
        raise HTTPException(status_code=404, detail=f"no match for area with ID {area_id}")
    finally:
        if conn:
            close_db_connection(conn)


"""
Error handling considerations for GET "/api/restaurants":
- path is incorrect; 404 default handled by FastAPI
- method does not exist; 405 default handled by FastAPI, 422 for POST requests as this is a valid endpoint
- server error; custom 500 implemented
"""
@app.get("/api/restaurants")
def read_restaurants():
    conn = None
    try:
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
        return {"restaurants": formatted_restaurants_data}
    finally:
        if conn:
            close_db_connection(conn)


"""
Error handling considerations for POST "/api/restaurants":
- path is incorrect; 404 default handled by FastAPI
- method does not exist; 405 default handled by FastAPI (except GET as this is a valid endpoint)
- parameter is wrong type; 422 default handled by FastAPI
- valid, but empty input; 422 default handled by FastAPI
- server error; custom 500 implemented
"""
class NewRestaurant(BaseModel):
    restaurant_name: str
    area_id: int
    cuisine: str
    website: str

@app.post("/api/restaurants", status_code=201)
def add_new_restaurant(new_restaurant: NewRestaurant):
    conn = None
    try:
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
        return {"restaurant": formatted_restaurant_data}
    finally:
        if conn:
            close_db_connection(conn)


"""
Error handling considerations for DELETE "/api/restaurants/:restaurant_id":
- path is incorrect; 404 default handled by FastAPI
- method does not exist; 405 default handled by FastAPI, 422 for PATCH as this is a valid endpoint
- parameter is wrong type; 422 default handled by FastAPI
- parameter does not exist; custom 404 implemented
- server error; custom 500 implemented
"""
@app.delete("/api/restaurants/{restaurant_id}", status_code=204)
def delete_restaurant(restaurant_id: int):
    conn = None
    try:
        conn = connect_to_db()
        returning_content = conn.run(f"""DELETE FROM restaurants WHERE restaurant_id = {literal(restaurant_id)} RETURNING *;""")
        if not returning_content:
            raise HTTPException(status_code=404, detail=f"no match for restaurant with ID {restaurant_id}")
    finally:
        if conn:
            close_db_connection(conn)


"""
Error handling considerations for PATCH "/api/restaurants/:restaurant_id":
- path is incorrect; 404 default handled by FastAPI
- method does not exist; 405 default handled by FastAPI (except DELETE as this is a valid endpoint)
- parameter is wrong type; 422 default handled by FastAPI
- parameter does not exist; custom 404 implemented
- server error; custom 500 implemented
"""
class UpdatedAreaCode(BaseModel):
    area_id: Optional[int] = None

@app.patch("/api/restaurants/{restaurant_id}")
def update_area_id(restaurant_id: int, updated_area_id: UpdatedAreaCode):
    if not dict(updated_area_id)["area_id"]:
        raise HTTPException(status_code=400, detail="received empty request body; body must contain correct fields")
    
    conn = None
    try: 
        conn = connect_to_db()
        restaurant_data = conn.run(f"""UPDATE restaurants SET area_id = {literal(updated_area_id.area_id)} WHERE restaurant_id = {literal(restaurant_id)} RETURNING *;""")[0]
        column_names = [c["name"] for c in conn.columns]
        formatted_restaurant_data = dict(zip(column_names, restaurant_data))
        return {"restaurant": formatted_restaurant_data}
    except IndexError:
        raise HTTPException(status_code=404, detail=f"no match for restaurant with ID {restaurant_id}")
    finally:
        if conn:
            close_db_connection(conn)


@app.exception_handler(DatabaseError)
def handle_db_error(request: Request, exc: DatabaseError):
    print(exc)
    raise HTTPException(status_code=500, detail="server error: issue logged for investigation")