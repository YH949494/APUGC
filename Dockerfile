FROM python:3.11-slim

WORKDIR /app

# remove any conflicting bson if it exists in base layers
RUN pip uninstall -y bson || true

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
