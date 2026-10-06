from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import os
from elasticsearch import Elasticsearch
from collections import Counter
import re
from pydantic import BaseModel

class OrderCreate(BaseModel):
    customer_id: int
    restaurant_id: int
    menu_item_id: int
    quantity: int
    description: str

es = Elasticsearch(
    "http://elasticsearch:9200"
)

app = FastAPI(title="FoodFlow API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        database=os.getenv("DB_NAME", "foodflow"),
        user=os.getenv("DB_USER", "foodflow"),
        password=os.getenv("DB_PASSWORD", "foodflow"),
    )


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/orders")
def get_orders():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            o.id,
            c.name AS customer,
            r.name AS restaurant,
            m.name AS menu_item,
            o.quantity,
            o.description,
            o.created_at
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        JOIN restaurants r ON o.restaurant_id = r.id
        JOIN menu_items m ON o.menu_item_id = m.id
        ORDER BY o.created_at DESC
    """)

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return [
        {
            "id": row[0],
            "customer": row[1],
            "restaurant": row[2],
            "menu_item": row[3],
            "quantity": row[4],
            "description": row[5],
            "created_at": row[6],
        }
        for row in rows
    ]


@app.get("/api/restaurants")
def get_restaurants():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, name, category
        FROM restaurants
        ORDER BY name
    """)

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return [
        {
            "id": row[0],
            "name": row[1],
            "category": row[2],
        }
        for row in rows
    ]

@app.get("/api/search")
def search_orders(q: str):

    response = es.search(
        index="foodflow_orders",
        query={
            "multi_match": {
                "query": q,
                "fields": [
                    "customer",
                    "restaurant",
                    "menu_item",
                    "description"
                ]
            }
        }
    )

    return [
        hit["_source"]
        for hit in response["hits"]["hits"]
    ]

@app.get("/api/analytics")
def analytics():

    total_response = es.count(
        index="foodflow_orders"
    )

    restaurant_response = es.search(
        index="foodflow_orders",
        size=0,
        aggs={
            "orders_by_restaurant": {
                "terms": {
                    "field": "restaurant",
                    "size": 10
                }
            }
        }
    )

    category_response = es.search(
        index="foodflow_orders",
        size=0,
        aggs={
            "orders_by_category": {
                "terms": {
                    "field": "category",
                    "size": 10
                }
            }
        }
    )

    return {
        "total_orders": total_response["count"],

        "orders_by_restaurant": [
            {
                "name": bucket["key"],
                "count": bucket["doc_count"]
            }
            for bucket in restaurant_response[
                "aggregations"
            ]["orders_by_restaurant"]["buckets"]
        ],

        "orders_by_category": [
            {
                "name": bucket["key"],
                "count": bucket["doc_count"]
            }
            for bucket in category_response[
                "aggregations"
            ]["orders_by_category"]["buckets"]
        ]
    }

@app.get("/api/wordcloud")
def wordcloud():

    response = es.search(
        index="foodflow_orders",
        size=1000
    )

    stop_words = {
        "and",
        "the",
        "please",
        "with",
        "no",
        "add",
        "make",
        "it"
    }

    words = []

    for hit in response["hits"]["hits"]:

        description = hit["_source"].get(
            "description",
            ""
        )

        extracted = re.findall(
            r"\b[a-zA-Z]+\b",
            description.lower()
        )

        for word in extracted:

            if word not in stop_words:
                words.append(word)

    counts = Counter(words)

    return [
        {
            "word": word,
            "count": count
        }
        for word, count in counts.most_common(20)
    ]

@app.post("/api/orders")
def create_order(order: OrderCreate):

    conn = psycopg2.connect(
        host="postgres",
        database="foodflow",
        user="foodflow",
        password="foodflow"
    )

    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO orders
        (customer_id, restaurant_id, menu_item_id, quantity, description)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            order.customer_id,
            order.restaurant_id,
            order.menu_item_id,
            order.quantity,
            order.description
        )
    )

    order_id = cursor.fetchone()[0]

    conn.commit()

    cursor.close()
    conn.close()

    return {
        "message": "Order created successfully",
        "order_id": order_id
    }
