FROM python:3.10

WORKDIR /usr/app/src

COPY requirements.txt requirements.txt

RUN  pip install --upgrade pip \
     && pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]