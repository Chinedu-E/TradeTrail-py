FROM python:3.9-slim-buster

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file to the container
COPY ./requirements.txt /app/requirements.txt

# Install the required packages
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

# Copy the rest of the application code to the container
COPY . ./app

WORKDIR /app/src
# Expose port 5000 for the FastAPI application
EXPOSE 5050

# Set the command to run when the container starts
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5050"]