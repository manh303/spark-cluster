import psycopg2
from psycopg2 import sql

host="localhost"
port="5434"
database="employee"
user="postgres"
password="1234"

conn=psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
cur = conn.cursor()

create_table_query = '''
Create TABLE IF NOT EXISTS sales_data (
    sale_id SERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    category_id INT NOT NULL,
    quantity INT NOT NULL,
    revenue DECIMAL NOT NULL,
    date DATE NOT NULL
);

Create TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category_id INT NOT NULL,
    price DECIMAL NOT NULL
);

Create TABLE IF NOT EXISTS categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL
);
'''

cur.execute(create_table_query)

# Commit các thay đổi vào cơ sở dữ liệu
conn.commit()

insert_sales_query = '''
INSERT INTO sales_data (product_id, category_id, quantity, revenue, date)
VALUES (%s, %s, %s, %s, %s);
'''
insert_products_query = '''
INSERT INTO products (product_id, product_name, category_id, price)
VALUES (%s, %s, %s, %s);
'''
insert_categories_query = '''
INSERT INTO categories (category_id ,category_name)
VALUES (%s, %s);
'''
data = [
    (101, 1, 5, 500, "2023-01-01"),
    (102, 1, 3, 300, "2023-02-01"),
    (103, 2, 8, 800, "2023-02-01"),
    (104, 1, 7, 700, "2023-03-01"),
    (105, 3, 10, 1000, "2023-03-01"),
    (106, 3, 4, 400, "2023-03-01"),
    (107, 1, 6, 600, "2023-04-01"),
    (108, 2, 5, 500, "2023-05-01"),
    (109, 3, 3, 300, "2023-06-01"),
    (110, 3, 2, 200, "2023-06-01"),  
]
data2 = [
    (101,"Product A", 1, 100),
    (102,"Product B", 1, 100),
    (103,"Product C", 2, 100),
    (104,"Product D", 3, 100),
    (105,"Product E", 3, 100)
]
data3 = [
    (1, "Category 1"),
    (2, "Category 2"),
    (3, "Category 3")
]
cur.executemany(insert_sales_query, data)
conn.commit()

cur.executemany(insert_products_query, data2)
conn.commit()

cur.executemany(insert_categories_query, data3)
conn.commit()
# Đóng cursor và kết nối
cur.close()
conn.close()

print("Table created and data inserted successfully!")