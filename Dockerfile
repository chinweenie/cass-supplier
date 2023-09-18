FROM python:3.9
WORKDIR /app
COPY . .
#RUN chmod +x init.sh
#CMD ["python", "app.py"]
#RUN sh init.sh
RUN apt clean && apt update && apt install -y vim

#RUN apt update && apt install vim
RUN pip install cassandra-driver
RUN pip install Cluster
RUN pip install cqlsh

CMD ["tail", "-f", "/dev/null"]

#COPY startup.cql /docker-entrypoint-initdb.d/
