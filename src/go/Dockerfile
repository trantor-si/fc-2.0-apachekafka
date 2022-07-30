FROM golang:1.16

WORKDIR /go/src
ENV PATH="/go/bin:${PATH}"

# librdkafka é a biblioteca para comunicação com o Kafka (feito em C++)
RUN apt-get update && \
    apt-get install build-essential librdkafka-dev -y

CMD ["tail", "-f", "/dev/null"]