# this image automatically installs the requirements from requirements.txt assuming it is in the root of the project
FROM bde2020/spark-python-template:3.2.1-hadoop3.2

# set the working directory
WORKDIR /app

# copy the source files
COPY . /app

RUN rm -rf /tmp/*

CMD ["spark-submit", "--master", "spark://spark-master:7077", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-avro_2.12:3.2.1,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0", "src/main.py"]
