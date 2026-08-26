#!/bin/bash
export $(grep -v '^#' .env | xargs)

mysql -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" < schema.sql && \
SPARK_HOME=$(python3 -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))") python3 spark_pipeline.py