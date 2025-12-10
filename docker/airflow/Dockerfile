FROM apache/airflow:2.9.1-python3.9

USER root
# Install tools + Postgres client libs
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
         libpq-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
# Copy requirements and install
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt