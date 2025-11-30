# Use an official Python runtime as a base image
FROM python:3.11.14-trixie

WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

SHELL ["/bin/bash", "-c"]

COPY generate_stock_data.py /tmp/generate_stock_data.py
COPY gen_weather_data.py /tmp/gen_weather_data.py

# Define the command to run your application
CMD ["python3", "/tmp/gen_weather_data.py"]