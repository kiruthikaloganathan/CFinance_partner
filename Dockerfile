FROM openjdk:8-jre-slim


# Install Python 3 and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install PySpark
RUN pip3 install pyspark==3.3.1
RUN pip3 install delta-spark==1.1.0
RUN mkdir -p /opt/spark/work && \
 chmod -R 777 /opt/spark/work

# Copy the PySpark script into the container
COPY employees.csv /data/employees.csv
COPY src/data_validation.py /src/

CMD ["spark-submit", "--packages", "io.delta:delta-core_2.12:1.1.0", "/src/data_validation.py"]
