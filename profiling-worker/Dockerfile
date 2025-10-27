FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY carbon_profiler.py .
CMD ["python", "carbon_profiler.py"]
