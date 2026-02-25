FROM python:3.11-slim

WORKDIR /api
COPY api/ .
RUN chmod -R 777 /api

RUN pip install -r requirements.txt

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
