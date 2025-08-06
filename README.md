# ETL Pipeline: 1C → PostgreSQL → BI

Проект автоматизации выгрузки данных из 1С в PostgreSQL.

## 🎯 Зачем?
Чтобы автоматизировать отчёты и избавиться от ручных выгрузок в Excel.

## 🛠️ Технологии
- Python
- PostgreSQL
- JSON (имитация 1С)
- ETL (Extract, Transform, Load)

## 🚀 Как запустить
1. Установи зависимости: `pip install -r requirements.txt`
2. Запусти: `python etl_1c_to_postgres.py`

## 📊 Результат
Данные попадают в PostgreSQL и готовы к визуализации в Qlik, Power BI и др.