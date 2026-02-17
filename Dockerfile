# Use slim Python image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y build-essential

# Copy project files
COPY pyproject.toml poetry.lock* /app/

# Install Poetry
RUN pip install poetry

# Disable virtualenv creation inside container
RUN poetry config virtualenvs.create false

# Install dependencies
RUN poetry install --without dev --no-interaction --no-ansi

# Copy rest of project
COPY . /app

# Expose port
EXPOSE 8000

# Start FastAPI
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]