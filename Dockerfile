FROM python:3.8-slim-buster

# Install OpenJDK and other dependencies
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Set up working directory
WORKDIR /app

# Spark installation
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV SPARK_MASTER_HOST=spark-master
ENV SPARK_WORKER_WEBUI_PORT=8081

# Create Spark directories
RUN mkdir -p $SPARK_HOME/jars \
    && mkdir -p $SPARK_HOME/logs \
    && mkdir -p $SPARK_HOME/work

# Download and set up Spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /tmp \
    && cp -r /tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/* $SPARK_HOME/ \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && rm -rf /tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}

# Download JDBC driver for PostgreSQL
RUN wget -P $SPARK_HOME/jars https://jdbc.postgresql.org/download/postgresql-42.7.5.jar

# Set permissions
RUN chmod -R 777 $SPARK_HOME

# Upgrade pip and install Python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy configuration files
COPY start-spark.sh /
COPY spark-defaults.conf $SPARK_HOME/conf/

# Make the start script executable
RUN chmod +x /start-spark.sh

ENTRYPOINT ["/start-spark.sh"]