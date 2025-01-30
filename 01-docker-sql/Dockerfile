FROM python:3.9.1

RUN apt-get install wget

RUN pip install --upgrade pip
# Install Jupyter and additional Python libraries
RUN pip install --no-cache-dir notebook pandas numpy sqlalchemy pyarrow psycopg2
# Set the working directory
WORKDIR /app

# Expose the default Jupyter Notebook port
EXPOSE 8888

# Command to run Jupyter Notebook
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
