# Use an official Python runtime as a parent image
FROM python:3.10

# Upgrade pip
RUN pip install --upgrade pip

# Set the working directory to /app
WORKDIR /app

# Copy all the contents of the current dir to /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

# Make port 5000 available to the world outside this container
EXPOSE 5000

# Set environment variables
ENV NAME myenv
ENV FLASK_APP app.py

# Run the Flask application
CMD ["python", "app.py"]