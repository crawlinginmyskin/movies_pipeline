FROM python:3.12-slim
WORKDIR /app
COPY dist/*.whl .
RUN pip3 install *.whl
CMD ["python3", "-m", "omdb_downloader.trigger"]