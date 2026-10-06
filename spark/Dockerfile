FROM apache/spark:3.5.6

USER root

RUN pip install --no-cache-dir \
    pandas \
    pyarrow \
    boto3 \
    "elasticsearch>=8,<9"

WORKDIR /opt/spark/jobs

COPY jobs/ .

CMD ["/opt/spark/bin/spark-submit","--packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.4","pipeline.py"]
