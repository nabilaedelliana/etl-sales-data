FROM python:3.9

RUN pip install --upgrade pip

RUN apt-get update && apt-get install -y tzdata
ENV TZ=Asia/Jakarta 

WORKDIR /app

# Copy requirements and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire solution
COPY . .

# Set environment variables
ENV DATA_DIR=/data

# Command to run the script
CMD ["python", "scripts/ETL.py"]
