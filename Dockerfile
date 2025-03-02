# RUN python -m venv soda_venv && source soda_venv/bin/activate && \
#     pip install --no-cache-dir soda-core-bigquery==3.0.45 &&\
#     pip install --no-cache-dir soda-core-scientific==3.0.45 && deactivate

# FROM python:3.10.12-slim

# # Update and install system packages
# RUN apt-get update -y && \
#     apt-get install --no-install-recommends -y -q \
#     git libpq-dev && \
#     apt-get clean && \
#     rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# # Set environment variables
# ENV DBT_DIR /dbt

# # Set working directory
# WORKDIR /dbt

# # Copy requirements
# COPY ./dbt_requirements.txt /dbt_requirements.txt

# # Install DBT
# RUN pip install -U pip
# RUN pip install -r /dbt_requirements.txt
# RUN pip install dbt

# # Add dbt_project_1 to the docker image
# COPY dbt ./dbt
# RUN ["dbt", "deps", "--project-dir", "./dbt"]




FROM python:3.10.12-slim

# Update and install system packages
RUN apt-get update -y && \
  apt-get install --no-install-recommends -y -q \
  git libpq-dev && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY dbt_requirements.txt .

RUN pip install -U pip
RUN pip install -r dbt_requirements.txt

RUN mkdir /root/.dbt

COPY dbt ./dbt
COPY dbt/profiles.yml /root/.dbt/profiles.yml

RUN ["dbt", "deps", "--project-dir", "./dbt"]

