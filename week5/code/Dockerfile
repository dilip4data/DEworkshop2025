# Use Ubuntu 20.04 LTS as base image
FROM ubuntu:20.04

# Avoid prompts from apt and set timezone
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=EST

# Install necessary packages
RUN apt-get update && \
    apt-get install -y wget tar git curl gnupg && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables for Java, Spark, and Conda
ENV JAVA_HOME=/opt/jdk
ENV SPARK_HOME=/opt/spark
ENV PATH="/opt/conda/bin:${JAVA_HOME}/bin:${SPARK_HOME}/bin:${PATH}"

# Install OpenJDK 11
RUN wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz -O /tmp/openjdk-11.tar.gz && \
    mkdir -p "$JAVA_HOME" && \
    tar --extract --file /tmp/openjdk-11.tar.gz --directory "${JAVA_HOME}" --strip-components 1 && \
    rm /tmp/openjdk-11.tar.gz

# Install Miniconda
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh && \
    bash /tmp/miniconda.sh -b -p /opt/conda && \
    rm /tmp/miniconda.sh

# Install Python 3.10, PySpark 3.3.2, Jupyter Lab, and Pandas 2.0.1
# Note: Update PySpark to match the Spark version if necessary
RUN conda install -y python=3.10 && \
    pip install pyspark==3.5.1 jupyterlab pandas==2.0.1

# Install Spark 3.5.1
RUN wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz -O /tmp/spark-3.5.1-bin-hadoop3.tgz && \
    mkdir -p "$SPARK_HOME" && \
    tar xzfv /tmp/spark-3.5.1-bin-hadoop3.tgz --strip-components=1 -C "$SPARK_HOME" && \
    rm /tmp/spark-3.5.1-bin-hadoop3.tgz

# Download and add the GCS connector jar to the Spark jars directory
RUN wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar -O $SPARK_HOME/jars/gcs-connector-latest-hadoop2.jar

# Add the Google Cloud SDK distribution URI as a package source
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -

# Install Google Cloud SDK
RUN apt-get update && \
    apt-get install -y google-cloud-sdk && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /opt/spark/work-dir

# Expose ports for Jupyter Lab and Spark UI
EXPOSE 8888 4040 8080 8081 18080

# Start Jupyter Lab by default
CMD ["jupyter", "lab", "--ip='0.0.0.0'", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''", "--NotebookApp.password=''"]