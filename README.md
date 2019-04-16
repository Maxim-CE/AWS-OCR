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
