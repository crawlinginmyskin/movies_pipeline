FROM python:3.12-slim
RUN pip install --upgrade pip
RUN pip install poetry
RUN mkdir -p /root/.dbt
COPY profiles.yml /home/.dbt/profiles.yml
COPY profiles.yml /root/.dbt/profiles.yml
WORKDIR /app/movies
COPY movies .
RUN poetry config virtualenvs.create false && poetry run pip install --upgrade setuptools && poetry install
RUN dbt clean
COPY start.sh .
RUN ["chmod", "+x", "./start.sh"]
ENTRYPOINT ["./start.sh"]
