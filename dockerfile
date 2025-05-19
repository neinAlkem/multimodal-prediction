FROM python:3.10 as base

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      unzip \
      openjdk-17-jdk \
      build-essential \
      software-properties-common \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Setup the directories for our Spark and Hadoop installations
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# Download and install Spark
RUN curl https://archive.apache.org/dist/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz -o spark-3.5.5-bin-hadoop3.tgz && \
    tar xvzf spark-3.5.5-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 && \
    rm -rf spark-3.5.5-bin-hadoop3.tgz && \
    curl https://dlcdn.apache.org/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz -o hadoop-3.4.1-bin.tar.gz && \
    tar xfz hadoop-3.4.1-bin.tar.gz --directory /opt/hadoop --strip-components 1 && \
    rm -rf hadoop-3.4.1-bin.tar.gz

FROM base as pyspark

# Install python deps
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Set JAVA_HOME environment variable
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"

# Add the Spark and Hadoop bin and sbin to the PATH variable.
# Also add $JAVA_HOME/bin to the PATH
ENV PATH="$SPARK_HOME/sbin:/opt/spark/bin:${PATH}"
ENV PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:${PATH}"
ENV PATH="${PATH}:${JAVA_HOME}/bin"

# Setup Spark related environment variables
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3
ENV HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"

# Add Hadoop native library path to the dynamic link library path
ENV LD_LIBRARY_PATH="$HADOOP_HOME/lib/native:${LD_LIBRARY_PATH}"

# Set user for HDFS and Yarn (for production probably not smart to put root)
ENV HDFS_NAMENODE_USER="root"
ENV HDFS_DATANODE_USER="root"
ENV HDFS_SECONDARYNAMENODE_USER="root"
ENV YARN_RESOURCEMANAGER_USER="root"
ENV YARN_NODEMANAGER_USER="root"

# Add JAVA_HOME to haddop-env.sh
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> "$HADOOP_HOME/etc/hadoop/hadoop-env.sh"

# COPY the appropriate configuration files to their appropriate locations
COPY yarn/spark-defaults.conf "$SPARK_HOME/conf/"
COPY yarn/*.xml "$HADOOP_HOME/etc/hadoop/"

# Make the binaries and scripts executable and set the PYTHONPATH environment variable
RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
#ENV PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH

RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
  chmod 600 ~/.ssh/authorized_keys

COPY ssh_config ~/.ssh/config

# Copy appropriate entrypoint script
COPY entryscript.sh entryscript.sh

EXPOSE 22

ENTRYPOINT ["./entryscript.sh"]