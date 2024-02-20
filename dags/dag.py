from bs4 import BeautifulSoup
import requests
import csv
from datetime import datetime
import psycopg2
from psycopg2 import sql, extras
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

class HtmlParser:
    product_id = 0

    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path
        self.data_list = []

    def extract_data_from_html(self, html_content, region, market):
        df_new = pd.DataFrame()
        soup = BeautifulSoup(html_content, 'html.parser')

        def extract_text(element, sel):
            selected_element = element.select_one(sel)
            if selected_element:
                return selected_element.get_text()
            else:
                print("Element not found for selector:", sel)
                return None


        products = soup.select("div.page_wrap")

        for product in products:
            name_selector = ".head_block"
            price_selector = ".new"

            name = extract_text(product, name_selector)
            price = extract_text(product, price_selector)

            # Генерируем уникальный идентификатор для продукта
            self.product_id += 1

            item = {
                "id": self.product_id,
                "region": region,
                "market": market,
                "name": name,
                "price": price,
                "date": datetime.now().strftime("%Y-%m-%d")
            }

            # Добавляем элемент в список
            self.data_list.append(item)

            
            columns = ["id", "region", "market", "name", "price", "date"]
            new_df = pd.DataFrame(self.data_list, columns=columns)

            if df_new.empty:
                df_new = new_df
            else:
                df_new = pd.concat([df_new, new_df], ignore_index=True)
        return df_new



    def process_urls(self):
        df_new = pd.DataFrame()

        try:
            # Чтение данных из CSV-файла
            with open(self.csv_file_path, newline="", encoding="utf-8") as csvfile:
                csv_reader = csv.reader(csvfile)
                # Пропускаем заголовки, если они есть
                next(csv_reader, None)
                # Итерация по строкам и добавление URL в список
                for row in csv_reader:
                    region = row[0]  
                    market = row[1]  
                    url = row[3] 

                    print(f"\nProcessing URL: {url}")

                    page = requests.get(url)
                    html_content = page.text 

                    # Извлечение данных о продуктах из HTML-кода
                    new_df = self.extract_data_from_html(html_content, region, market)
                    df_new = pd.concat([df_new, new_df], ignore_index=True)

        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        return df_new

def load_data_to_grocery_list_table(**kwargs):

    csv_file_path = "C:\\Users\\Bekarys Dauletov\\Desktop\\market_data_ZKO.csv"
    html_parser = HtmlParser(csv_file_path)
    df_new = html_parser.process_urls()

    # Загрузка данных в PostgreSQL
    conn = psycopg2.connect(
        dbname='postgres',
        user='nurgisa',
        password='qmNKXO',
        host='192.168.149.52',
        port='5432'
    )

    cur = conn.cursor()

    table_name = 'markets.grocery_list'

    try:
        
        for index, row in df_new.iterrows():
            
            cur.execute(
                f"""
                INSERT INTO {table_name} (id, region, market, name, price, date)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET 
                    id = EXCLUDED.id,
                    region = EXCLUDED.region,
                    market = EXCLUDED.market,
                    name = EXCLUDED.name,
                    price = EXCLUDED.price, 
                    date = EXCLUDED.date
                """,
                (
                    row["id"],
                    row["region"],
                    row["market"],
                    row["name"],
                    row["price"],
                    row["date"]
                )
            )
        conn.commit()
        print("Данные успешно загружены в PostgreSQL")
    except Exception as e:
        print(f"Произошла ошибка при загрузке данных в PostgreSQL: {e}")

    cur.close()
    conn.close()
#load_data_to_grocery_list_table()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG('markets.grocery_list', default_args=default_args, schedule_interval='0 15 * * *')

load_data_to_grocery_list_table_task = PythonOperator(
    task_id='load_data_to_grocery_list_table',
    python_callable=load_data_to_grocery_list_table,
    provide_context=True,
    dag=dag
)

create_table_query = """
                    CREATE TABLE IF NOT EXISTS markets.grocery_list (
                        id SERIAL PRIMARY KEY,
                        region VARCHAR(255),
                        market VARCHAR(255),
                        name VARCHAR(255),
                        price VARCHAR(255), 
                        date DATE
                    )
                """

create_table_task = PostgresOperator(
    task_id='create_table_task',
    postgres_conn_id='Goszakup',
    sql=create_table_query,
    dag=dag
)

create_table_task >> load_data_to_grocery_list_table_task
