# AWS-OCR
In this project the optical character recognition (OCR) algorithms distributively applied on images via AWS instances. Each instance will process part of images out of the total batch. 
By the end of the process, each instance saves the processed images within shared html file.
The project is composed of three main parts, local application`(Local.java)`, manager `(MrManager.java)` and workers `(Worker.java)`.

# Local Application
This application starts working on your local machine (not AWS cloud) as follows:
1. Uploads `links.txt` file to S3 storage which contains links to images.
2. Send a message that stating the location of `links.txt` in S3 bucket to an SQS queue.
3. Initializing the manager.
4. Wait for the manager and the workers to finish the OCR process by checking specific SQS queue.
5. Download `MrManagerOutput.html` file from S3, the file contains images with ORC output.

# Manager
Unlike the local application, the manager resides on EC2 node. It's main purpose is to initialize the workers and controling their work via SQS messages and S3 buckets:
1. Receive message from local application via SQS.
2. Download `links.txt` from S3 bucket.
3. Send each link from `links.txt` file to SQS queue.
4. Create worker for every n messages.
5. Wait for workers to finish their job, terminate the workers when theyr done.
6. Read all messages from the results queue, create and upload `MrManagerOutput.html` to S3 bucket.
7. Send message to local application via SQS queue that indicates the end of OCR process.

# Worker
Similarly to the manager, this process resides on EC2 node as follows:
1. Obtain link to an image via SQS queue and download the image.
2. Initiate OCR algorithm on the image.
3. Notify the manager of the text exctracted from the image.
4. Remove the image from SQS queue.
5. Repeat the process.

# How to use
1. In order to use AWS cloud [register here](https://aws.amazon.com)
2. Download and install [Java SDK](https://aws.amazon.com/sdk-for-java/)
3. Create a project with provided `.java` files from this repository.
4. Export each `.java` file as `.jar`.
5. Create S3 bucket named "shleem" and upload `MrManager.jar` & `Worker.jar` to this bucket.
6. ```java -jar yourjar.jar inputFileName n``` for example: ```java -jar Local.jar links.txt 10```
