FROM python:3.10
WORKDIR /app
ADD requirement.txt .
RUN pip install -r requirement.txt
ADD src ./src
CMD ["python", "src/schedular.py"]
