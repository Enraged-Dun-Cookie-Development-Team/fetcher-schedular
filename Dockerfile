FROM python:3.10
WORKDIR /app
ADD requirement.txt .
RUN pip install -r requirement.txt
RUN pip install --pre grpcio

ADD src ./src
CMD ["python", "src/schedular.py"]
