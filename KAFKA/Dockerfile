FROM python:3.9 as kafka_app
WORKDIR /code
COPY ./code/ /code/
RUN pip install -r requirements.txt
CMD ["bash"]
#CMD ["nohup", "python","api.py",">log.txt", "2>&1", "&"]
#CMD ["read", "a"]
#CMD ["nohup", "python","api.py",">log.txt", "2>&1", "&"]
#CMD ["bash"]
#ENTRYPOINT ["tail", "-f", "/dev/null"]