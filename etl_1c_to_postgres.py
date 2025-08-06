# etl_1c_to_postgres.py
import json
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# 🔹 1. ИЗВЛЕЧЕНИЕ (Extract) — читаем данные из 1С (JSON)
def extract_1c_data(file_path):
    print("🔹 Извлечение данных из 1С...")
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    print(f"✅ Загружено {len(df)} строк")
    return df

# 🔹 2. ОБРАБОТКА (Transform) — очистка и преобразование
def transform_data(df):
    print("🔹 Обработка данных...")
    
    # Приведём дату к формату datetime
    df['Дата'] = pd.to_datetime(df['Дата'])
    
    # Сумма — строка с пробелами? Приведём к float
    df['Сумма'] = df['Сумма'].astype(str).str.replace(' ', '').astype(float)
    
    # Добавим вычисляемые поля
    df['Год'] = df['Дата'].dt.year
    df['Месяц'] = df['Дата'].dt.month
    df['Выручка'] = df['Сумма']
    df['Средняя_цена'] = df['Сумма'] / df['Количество']
    
    # Уберём дубли (на всякий случай)
    df.drop_duplicates(inplace=True)
    
    print("✅ Данные обработаны")
    return df

# 🔹 3. ЗАГРУЗКА (Load) — в PostgreSQL
def load_to_postgres(df, table_name='sales'):
    print("🔹 Загрузка в PostgreSQL...")
    
    # 🔧 Настрой подключения (измени под себя)
    DATABASE_URL = "postgresql://user:password@localhost:5432/analytics"
    
    try:
        engine = create_engine(DATABASE_URL)
        df.to_sql(table_name, engine, if_exists='append', index=False, method='multi')
        print(f"✅ Данные загружены в таблицу '{table_name}'")
    except Exception as e:
        print(f"❌ Ошибка при подключении к PostgreSQL: {e}")

# 🔹 4. ОСНОВНОЕ ВЫПОЛНЕНИЕ
if __name__ == "__main__":
    print("🚀 Запуск ETL-пайплайна: 1C → PostgreSQL")
    
    try:
        # Путь к файлу выгрузки из 1С
        file_path = '1c_sales.json'
        
        # Выполнение ETL
        raw_data = extract_1c_data(file_path)
        cleaned_data = transform_data(raw_data)
        load_to_postgres(cleaned_data, table_name='sales')
        
        print("🎉 ETL-пайплайн завершён успешно!")
        
        # Показать первые строки
        print("\n📊 Пример данных:")
        print(cleaned_data.head())
        
    except Exception as e:
        print(f"💥 Ошибка в пайплайне: {e}")