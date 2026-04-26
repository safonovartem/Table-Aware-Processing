# import os
# import json
# import pandas as pd
# import numpy as np
# #from src.main import process_dataframe
#
#
# def create_stress_test_file(filepath: str, rows: int = 10000):
#     """Генерирует Excel файл на 10k строк для проверки MVP."""
#     print(f"⏳ Генерация файла {filepath} на {rows} строк...")
#     df = pd.DataFrame({
#         "ID": range(1, rows + 1),
#         "Product": [f"Item_{i}" for i in range(rows)],
#         "Price": np.random.uniform(10.0, 500.0, rows).round(2),
#         "In_Stock": np.random.choice([True, False], rows),
#         "Date": pd.date_range(start="2023-01-01", periods=rows)
#     })
#
#     # Искусственно добавляем пустые значения для проверки профилировщика
#     df.loc[10:50, 'Price'] = np.nan
#
#     df.to_excel(filepath, index=False, sheet_name="StressData")
#     print("✅ Файл сгенерирован!")
#     return df
#
#
# def run():
#     os.makedirs("output", exist_ok=True)
#     os.makedirs("examples", exist_ok=True)
#
#     stress_file = "examples/03_stress_test_10k.xlsx"
#     df = create_stress_test_file(stress_file)
#
#     print("⏳ Запуск обработки чанков (max_rows=500)...")
#     # Используем функцию из твоего main.py
#     result = process_dataframe(
#         df=df,
#         filename="03_stress_test_10k.xlsx",
#         sheet_name="StressData",
#         max_rows_per_chunk=500,
#         max_cells_per_chunk=5000
#     )
#
#     output_path = "output/stress_test_result.json"
#     with open(output_path, "w", encoding="utf-8") as f:
#         json.dump(result, f, ensure_ascii=False, indent=2, default=str)
#
#     print(f"✅ Готово! Результат сохранен в {output_path}")
#     print(f"📊 Всего чанков создано: {len(result['chunks'])}")


# if __name__ == "__main__":
#     run()