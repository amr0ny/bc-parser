FROM python:3.12-alpine

WORKDIR /opt/bc-parser
COPY requirements.txt requirements.txt
RUN chmod +x .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . .

ENTRYPOINT [ "python", "/opt/bc-parser/app.py"]