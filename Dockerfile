FROM golang:1.21-alpine

LABEL version="0.1"
LABEL description="Amazon DynamoDB Delete Partition binary utility"

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . ./

RUN go build -o /dynamoctl /app/cmd/main.go

CMD [ "/dynamoctl" ]

