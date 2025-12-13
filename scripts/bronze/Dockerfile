FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir pandas pyarrow openpyxl lxml
COPY scripts/bronze/modded_bronzeval.py . 
CMD ["python", "modded_bronzeval.py"]