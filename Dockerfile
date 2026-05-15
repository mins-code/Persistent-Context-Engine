FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies needed for runtime shell execution
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    && rm -rf /var/lib/apt/lists/*

# Copy your requirements.txt first and install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your codebase into the container
COPY . .

# Ensure the submission execution script has proper run permissions
RUN chmod +x bench/run.sh

# Define the default entrypoint command that the judges will execute
ENTRYPOINT ["/bin/bash", "bench/run.sh"]