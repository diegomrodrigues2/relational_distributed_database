FROM python:3.10-slim

# Install node for the React frontend
RUN apt-get update && \
    apt-get install -y nodejs npm && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install frontend dependencies
RUN cd app && npm install --legacy-peer-deps

EXPOSE 50051 8000

ENTRYPOINT ["python", "start_node.py"]
