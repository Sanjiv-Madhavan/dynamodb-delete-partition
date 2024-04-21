## Problem
Develop code to build a binary called dynamoctl which is an extension of aws dynamodb command line tool used to delete table partitions from Amazon's DynamoDB database. Advantage of this command is you do not have to write code to batch write items with putRequests and deleteRequests and configure them according to aws rules. You can blindly choose a Partition Key and delete entries in the partition key, accordingly. Further advantage is that if a partition key contains a myriad entries, the goroutines would delete them quickly with the principles of channels and concurrency.

## Solution Proposed
Use queryPaginator to delete table partitions in batches of configurable size. The goroutines used to build the tool would delete the table page items in the form of batch writes. All operations are performed by leveraging the power of [Aws sdk for go](https://docs.aws.amazon.com/sdk-for-go/). Refer below for further sdk details

### SDK used is amazon's go sdk for dynamoDB

- [Getting started with sdk](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.html)
- [API reference for dynamoDB operations performed in this repository](https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#pkg-types)

## Build command
### Local
To create a bin, use the command <br>

```bash
 go build -o bin/dynamoctl ./cmd/main.go 
 ```

 After building the bin, use the below command to delete a partition

 ```bash
$ ddbctl delete-partition --table-name <<table-name>> --partition-value <<partition-value>> --endpoint-url <<optional-endpoint-url> --region <<optional-aws-region>>

$ # skip confirmation
$ ddbctl delete-partition --table-name <<table-name>> --partition-value <<partition-value>> --endpoint-url <<optional-endpoint-url> --region <<optional-aws-region>> --skip-confirmation
```

### Sample workflow
- Create table
```bash
aws dynamodb create-table --table-name Orders --attribute-definitions AttributeName=orderId,AttributeType=S --key-schema AttributeName=orderId,KeyType=HASH --billing-mode PAY_PER_REQUEST --endpoint-url http://localhost:8000 --region us-east-1
```

- Insert table item
```bash
aws dynamodb put-item \    --table-name Orders \                                                                                  
    --item '{"orderId": {"S": "1"}, "productName": {"S": "Product A"}, "quantity": {"N": "2"}, "price": {"N": "10.99"}}' \
    --endpoint-url http://localhost:8000 \
    --region us-east-1
```

- Delete using dynamoctl extension
```bash
bin/dynamoctl delete-partition --table-name Orders --partition-value "1" --endpoint-url http://localhost:8000 --region us-east-1 --skip-confirmation 
```

### For my reference:
#### commands used:

- Create table
```bash
aws dynamodb create-table --table-name Orders --attribute-definitions AttributeName=orderId,AttributeType=S --key-schema AttributeName=orderId,KeyType=HASH --billing-mode PAY_PER_REQUEST --endpoint-url http://localhost:8000 --region us-east-1
```

- insert into table
```bash
aws dynamodb put-item --table-name Orders --item '{"orderId": {"S": "1"}, "productName": {"S": "Product A"}, "quantity": {"N": "2"}, "price": {"N": "10.99"}}' --endpoint-url http://localhost:8000 --region us-east-1
```

- Delete partition 

```
- go build -o bin/dynamoctl ./cmd/main.go
- bin/dynamoctl delete-partition --table-name Orders --partition-value "1" --endpoint-url http://localhost:8000 --region us-east-1 --skip-confirmation
