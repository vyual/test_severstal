from datetime import datetime
import io
import os
import psycopg2
import redis
import time
import random
import threading
from PIL import Image

# Подключаемся к базе данных Postgresql
conn = psycopg2.connect(host="localhost", database="test_severstal", user="postgres", password="12345678")
cur = conn.cursor()

# Создаем таблицу для хранения информации о сохраненных изображениях
cur.execute("""
    CREATE TABLE IF NOT EXISTS images  (
        id SERIAL PRIMARY KEY,
        filename VARCHAR(255) NOT NULL,
        timestamp VARCHAR(255) NOT NULL,
        size INTEGER NOT NULL
    )
""")
conn.commit()

# Создаем соединение с Redis
r = redis.Redis(host='localhost', port=6379, db=0)

img_folder = "img"  # путь к папке с изображениями
img_files = os.listdir(img_folder)  # список файлов в папке


# Функция, которая добавляет числа в очередь Redis
def producer():
    if not img_files:
        print("No images found in the folder")
        return  # остановить функцию, если файлов нет
    while True:
        if not img_files:  # проверяем, если файлов больше нет, то останавливаем цикл
            break
        img_file = random.choice(img_files)  # выбираем случайное изображение из папки
        img_path = os.path.join(img_folder, img_file)  # получаем полный путь к изображению
        with open(img_path, "rb") as f:
            img_data = f.read()  # читаем содержимое изображения в бинарном виде
        r.lpush('images', img_data)  # добавляем содержимое изображения в очередь Redis
        print(f'Added image {img_file} to the queue')
        img_files.remove(img_file)  # удаляем добавленный файл из списка файлов
        time.sleep(1)


# Функция, которая забирает числа из очереди Redis и сохраняет их в базе данных Postgresql
def consumer():
    while True:
        # Получаем следующее изображение из очереди Redis
        image = r.brpop('images', timeout=0)
        if not image:  # если очередь пустая, то выходим из цикла
            break
        # Преобразуем байты в изображение
        img = Image.open(io.BytesIO(image[1]))
        timestamp_nostr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        size = len(image[1])
        timestamp = timestamp_nostr.replace(":", "-")
        filename = f"{timestamp}.png"

        img.save(filename)

        print(f'Added image {filename} to the psql database')
        # Сохраняем информацию об изображении в базе данных Postgresql
        cur.execute("INSERT INTO images (filename, timestamp, size) VALUES (%s, %s, %s)", (filename, timestamp, size))
        conn.commit()


def flush_redis_dbs():
    r = redis.Redis(host='localhost', port=6379, db=0)
    r.flushdb()


flush_redis_dbs()

# Создаем два потока для работы с Redis
producer_thread = threading.Thread(target=producer)
consumer_thread = threading.Thread(target=consumer)

# Запускаем потоки
producer_thread.start()
consumer_thread.start()

# Ждем, пока потоки завершат работу
producer_thread.join()
consumer_thread.join()
