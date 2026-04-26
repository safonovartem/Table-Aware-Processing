import io
from typing import List, Dict, Any

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse

app = FastAPI(
    title="Приложение команды CPR",
    description="Модуль для превращения .xlsx и .csv в осмысленные чанки для индексации",
    version="1.0.0.1",
)


def process_dataframe(
    df: pd.DataFrame, filename: str, sheet_name: str, chunk_size: int = 100
) -> List[Dict[str, Any]]:
    """
    Разбивает DataFrame на логические чанки.
    Использует Markdown-форматирование для сохранения структуры таблицы.
    """
    chunks = []

    # Очистка данных: заполняем пустые ячейки, чтобы не терять структуру
    df = df.fillna(value="[ПУСТО]")

    # Извлекаем типы данных колонок (приводим к строке для JSON)
    column_types = {str(col): str(dtype) for col, dtype in df.dtypes.items()}
    total_rows = len(df)

    # Бьем таблицу на чанки по `chunk_size` строк
    for start_idx in range(0, total_rows, chunk_size):
        end_idx = min(start_idx + chunk_size, total_rows)
        sub_df = df.iloc[start_idx:end_idx]

        # Превращаем кусок таблицы в Markdown
        markdown_table = sub_df.to_markdown(index=False)

        # Формируем самодостаточный текст для LLM
        chunk_text = (
            f"Источник данных: файл '{filename}', лист '{sheet_name}'.\n"
            f"Диапазон строк: {start_idx + 1} - {end_idx} (из {total_rows}).\n"
            f"Структура данных:\n{markdown_table}"
        )

        # Сохраняем метаданные отдельно для векторной БД (помогает при фильтрации)
        metadata = {
            "source_file": filename,
            "sheet_name": sheet_name,
            "start_row": start_idx + 1,
            "end_row": end_idx,
            "columns": list(df.columns.astype(str)),
            "column_types": column_types,
        }

        chunks.append({"text": chunk_text, "metadata": metadata})

    return chunks


@app.post("/upload-table/", summary="Загрузить CSV/XLSX и получить чанки")
async def upload_table(file: UploadFile = File(...)):
    filename = file.filename
    content = await file.read()

    chunks = []

    try:
        if filename.endswith(".csv"):
            # Читаем CSV. Используем utf-8 по умолчанию, можно добавить логику определения кодировки
            df = pd.read_csv(io.BytesIO(content))
            chunks.extend(process_dataframe(df, filename, sheet_name="CSV_Data"))

        elif filename.endswith((".xlsx", ".xls")):
            # Выбираем движок в зависимости от расширения
            engine = "openpyxl" if filename.endswith(".xlsx") else "xlrd"

            # sheet_name=None заставляет pandas прочитать ВСЕ листы в виде словаря {имя_листа: DataFrame}
            excel_data = pd.read_excel(
                io.BytesIO(content), sheet_name=None, engine=engine
            )

            for sheet_name, df in excel_data.items():
                if df.empty:
                    continue
                sheet_chunks = process_dataframe(df, filename, sheet_name)
                chunks.extend(sheet_chunks)
        else:
            raise HTTPException(
                status_code=400,
                detail="Поддерживаются только форматы .csv, .xlsx, .xls",
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка обработки файла: {str(e)}")

    return JSONResponse(content={"total_chunks": len(chunks), "chunks": chunks})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
