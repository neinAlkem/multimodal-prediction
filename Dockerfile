# Use a lightweight Python 3.11 image based on Debian Bullseye as the base image
FROM python:3.13-slim-bullseye as spark-base
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    sudo \                
    curl \                
    vim \                 
    unzip \               
    rsync \               
    openjdk-11-jdk \      
    build-essential \     
    software-properties-common \ 
    python3-setuptools \
    python3-distutils \
    python3-pip \
    ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
ENV PATH="$JAVA_HOME/bin:$PATH"

# Set environment variables for Spark and Hadoop
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

# Create necessary directories for Spark and Hadoop
RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# Download and install Spark using the specified SPARK_VERSION
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz -o spark-3.5.5-bin-hadoop3.tgz \
 && tar xvzf spark-3.5.5-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark-3.5.5-bin-hadoop3.tgz 

# Install required Python dependencies
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Add Spark paths to the environment variables
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_HOME="/opt/spark"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST=spark-master
ENV SPARK_MASTER_PORT=7077
ENV PYSPARK_PYTHON=python3  

# Copy Spark configuration file
COPY conf/spark-defaults.conf "$SPARK_HOME/conf"

# Grant execution permissions to Spark scripts
RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

# Add PySpark to PYTHONPATH to allow Spark module imports in Python
ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

# Create and copy directory for credentials
RUN mkdir -p /opt/spark/credentials

COPY credentials/credential.json /opt/spark/credentials/credential.json
ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/spark/credentials/credential.json

# Copy the entrypoint script that will start the required services
COPY start-spark.sh .
RUN chmod +x start-spark.sh

# Define the container entrypoint
ENTRYPOINT ["./start-spark.sh"]