# Use an official Python runtime as a base image
FROM python:3.11.14-trixie


# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY generate_stock_data.py /tmp/generate_stock_data.py

# Define the command to run your application
CMD ["python3", "/tmp/generate_stock_data.py"]