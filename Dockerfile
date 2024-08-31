FROM python:3.10
WORKDIR /app
ADD requirement.txt .
RUN pip install -r requirement.txt
RUN pip install --pre grpcio
RUN pip list
ADD src ./src
ADD Ceobe_Proto ./Ceobe_Proto
ADD conf ./conf
ADD ml_model ./ml_model

CMD ["python", "src/schedular.py"]
