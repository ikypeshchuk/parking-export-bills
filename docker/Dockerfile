FROM python:3.11

ENV ROOT_DIR /usr/src/app
COPY . $ROOT_DIR

COPY ./requirements.txt /requirements.txt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt && \
    rm -f /requirements.txt

WORKDIR $ROOT_DIR

CMD [ "python", "run.py" ]
