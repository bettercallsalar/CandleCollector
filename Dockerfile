# Use an official Python runtime as a base image.
FROM python:3.9-slim

# Set the working directory in the container.
WORKDIR /app

# Copy the requirements.txt file first (this allows Docker to cache installed dependencies)
COPY requirements.txt .

# Install the required Python packages.
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code.
COPY . .

# (Optional) Expose a port if your app serves HTTP; otherwise, not required.
# EXPOSE 8000

# Define the default command to run your application.
CMD ["python", "main.py"]
