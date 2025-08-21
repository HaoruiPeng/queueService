# Failures and problems on the first implementation

## Health check

I didn't implement health check servers on both writer and reader. But I didn't disable livenessProbe and readinessProbe in helm, which causes issues when deployed the application on k8s. Both pods were not Ready due to health check failure and kept restarting once deployed.

## Scalability and reliability

When I started the implementation, I was think to just send each file line by line. Then I realized that this is not working if my reader scaled up to more than 1 pod.

When multiple readers consume from the same queue, the lines from same file would be sent to different reader files, and they couldn't resemble the file into one before upload to the bucket.

The fix is to send the msg as json and put file name in the queue as well. However I didn't finish the implementation in writer.

Except for the file format, I also need to use exchange instead of queue for sending the line contents. By assigning the routing_key with unique file names, exchange guarantees that the lines from the same file will be sent to the same reader pod's queue.

