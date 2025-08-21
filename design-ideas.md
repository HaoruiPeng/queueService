# Design alternatives and other unfinished ideas

## File storage

Here I used objectStorage to storage the files to write to the queue. Which turned out a bad choice. 

The initial idea was to use services like SQS to trigger events so the writer can start fetch the file and write to the queue. But my cluster is not on AWS.

A much easier alternative is to use PV and mount the FS of writer app on it. So the files can be by the writer directly.

## Auto-scaling

I did have HPA in my helm chart. But I didn't manage to trigger scaling-up in my test due to time limit.

The original idea was to deploy the writer and reader as batch jobs. So the apps can be scaled up based on the number of files in the storage. To achieve this I was thinking to use KEDA for event triggering. But it is quite consuming to configure KEDA to run on my dev cluster due to tons of gatekeeper rules, so I had to drop the implementation.

## File buffer

The implementation of the file buffer in the reader is to buffer a whole file before writing to the destination S3 bucket. A more native solution should be using redis. 

## Tests

I didn't write any test due to time limit, but here are some test cases I was think to implement:

### Unit test

#### writer

- When the file provided in the queue does not exists in the storage
- Wrong queue names
- Wrong source S3 bucket name
- empty file
- non UTF-8 characters

#### reader

- Received empty file
- The received payload misses certain key
- Empty file
- Wrong queue name
- Wrong destination S3 bucket name 

### Integration test

- Upload a file and send the name to the "file-storage"  queue.
- Upload multiple files
- Run multiple replicas of reader pods
- restart the RabbitMQ pods and check if both writer and reader can connect back to it

### E2E test

- Test on HPA
  - Large file transmission
  - Large amount of files transmission
- Duplicated files

