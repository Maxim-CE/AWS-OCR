package com.amazonaws.samples;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.util.Base64;
 

public class Local {

	private static AmazonEC2 ec2 = null;
	private static List<Instance> instances  = null;
	
    public static void main(String[] args) throws Exception {
//Get credentials.
        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1") // Possible to use east-1 only, default.
                .build();
        IamInstanceProfileSpecification iamProfile = new IamInstanceProfileSpecification();
    	iamProfile.setName("MrManager"); // Role for Manager creation.
//Get access to AmazonS3.
        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
        		.withCredentials(credentialsProvider) //Comment for use in cloud.
        		.withRegion("us-east-1")
        		.build();
        String bucketName = "shleem";
        String filePath = args[0];
        String imagesPerWorker = args[1];
        System.out.println("Number of images per worker: " + imagesPerWorker + "\n");
//Get access to AmazonSQS.
        AmazonSQS sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(credentialsProvider) //Comment for use in cloud.
                .withRegion("us-east-1")
                .build();
        String queue = sqs.getQueueUrl("newTask").getQueueUrl(); //Get access to SQS to specific queue.
        String doneTask = sqs.getQueueUrl("doneTask").getQueueUrl(); 
        String urlsLocation = "shleem-links.txt " + imagesPerWorker;
        String htmlLocation;
//Logic starts here.        
    	uploadFile(filePath, bucketName, s3);
    	sendMessage(sqs, queue, urlsLocation);
    	createInstace(ec2, iamProfile);//------------------------------------------------------------------------MrManager creation-----------
    	
//Waiting for workers and manager to finish their job, should be blocked on doneTask queue.
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");  
        Date date = new Date();
        System.out.println("Local-Application blocking starts at: " + formatter.format(date) + "\n");
    	List<Message> completeFile = null;
    	while ((completeFile = receiveMessage(sqs, doneTask)).isEmpty());
    	date = new Date();
    	htmlLocation = completeFile.get(0).getBody();
    	deleteMessage(sqs, doneTask, completeFile);
    	System.out.println("Message received: " + htmlLocation + "\nLocal-Application blocking ends at: " + formatter.format(date) + "\n");
    	bucketName = htmlLocation.substring(0, 6);
        String fileName = htmlLocation.substring(7, htmlLocation.length());
        downloadObject(s3, bucketName, fileName);
        System.out.println("---File downloaded, termination initializes---\n");
        Terminator();
       
    } // Main
    
    public static void createInstace(AmazonEC2 ec2, IamInstanceProfileSpecification iamProfile) {
    	
    	 try {
             // Basic 32-bit Amazon Linux AMI 1.0 (AMI Id: ami-08728661)
               RunInstancesRequest request = new RunInstancesRequest("ami-1853ac65", 1, 1);
               request.setInstanceType(InstanceType.T2Micro.toString());
               request.withKeyName("secKey");
               request.setIamInstanceProfile(iamProfile);
               request.setUserData(getUserDataScript());
               instances = ec2.runInstances(request).getReservation().getInstances();
               System.out.println("Launch instances: " + instances);
           } 
           catch (AmazonServiceException ase) {
               System.out.println("Caught Exception: " + ase.getMessage());
               System.out.println("Reponse Status Code: " + ase.getStatusCode());
               System.out.println("Error Code: " + ase.getErrorCode());
               System.out.println("Request ID: " + ase.getRequestId());
           }
    }

    public static void Terminator() {
    	
    	for (Instance instance : instances) {
            TerminateInstancesRequest TerminateRequest = new TerminateInstancesRequest().withInstanceIds(instance.getInstanceId());
            ec2.terminateInstances(TerminateRequest);
        }
    }
    
    public static void uploadFile(String filePath, String bucketName, AmazonS3 s3) {
    	System.out.println("Uploading a new object to S3 from a file\n");
    	try {
    		File dir = new File(filePath);
    		String key;
    		for (File file : dir.listFiles()) {
    			key = file.getName().replace('\\', '_').replace('/','_').replace(':', '_');
    			PutObjectRequest req = new PutObjectRequest(bucketName, key, file);
    			System.out.println(filePath + " " + key + " " + file + "\n");   
    			s3.putObject(req);
            }
        }
	    catch (AmazonServiceException ase) {
	        System.out.println("Caught an AmazonServiceException, which means your request made it "
	                + "to Amazon S3, but was rejected with an error response for some reason.");
	        System.out.println("Error Message:    " + ase.getMessage());
	        System.out.println("HTTP Status Code: " + ase.getStatusCode());
	        System.out.println("AWS Error Code:   " + ase.getErrorCode());
	        System.out.println("Error Type:       " + ase.getErrorType());
	        System.out.println("Request ID:       " + ase.getRequestId());
	    } 
        catch (AmazonClientException ace) {
	        System.out.println("Caught an AmazonClientException, which means the client encountered "
	                + "a serious internal problem while trying to communicate with S3, "
	                + "such as not being able to access the network.");
	        System.out.println("Error Message: " + ace.getMessage());
	    }
    } // uploadFile
    
    public static void downloadObject(AmazonS3 s3, String bucketName, String fileName) {
    	

	    try {
	    	System.out.println("Downloading an object");
	        //S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
	    	S3Object object = s3.getObject(new GetObjectRequest(bucketName, fileName));
	        System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
	        InputStream objectData = object.getObjectContent();
    		Files.copy(objectData, new File(fileName).toPath());
    		objectData.close();
	        //displayTextInputStream(object.getObjectContent());
	    }
	    catch (AmazonServiceException ase) {
	        System.out.println("Caught an AmazonServiceException, which means your request made it "
	                + "to Amazon S3, but was rejected with an error response for some reason.");
	        System.out.println("Error Message:    " + ase.getMessage());
	        System.out.println("HTTP Status Code: " + ase.getStatusCode());
	        System.out.println("AWS Error Code:   " + ase.getErrorCode());
	        System.out.println("Error Type:       " + ase.getErrorType());
	        System.out.println("Request ID:       " + ase.getRequestId());
	    } 
	    catch (AmazonClientException ace) {
	        System.out.println("Caught an AmazonClientException, which means the client encountered "
	                + "a serious internal problem while trying to communicate with S3, "
	                + "such as not being able to access the network.");
	        System.out.println("Error Message: " + ace.getMessage());
	    } catch (IOException e) {
			e.printStackTrace();
		}
    } // downloadObject
    
    public static void sendMessage(AmazonSQS sqs, String queue,String message) {
		try {
        // Send a message
        System.out.println("Sending a message to MyQueue.\n");
       // sqs.sendMessage(new SendMessageRequest(myQueueUrl, "This is my message text."));
        sqs.sendMessage(new SendMessageRequest(queue, message));
        // Receive messages
        System.out.println("Receiving messages from MyQueue.\n");
        
        /*							
        ReceiveMessageRequest receiveMessageRequest1 = new ReceiveMessageRequest(myQURL);
        ReceiveMessageRequest receiveMessageRequest2 = new ReceiveMessageRequest(myQURL);
        ReceiveMessageRequest receiveMessageRequest3 = new ReceiveMessageRequest(myQURL);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest1).getMessages();
        messages.add(sqs.receiveMessage(receiveMessageRequest2).getMessages().get(0));
        messages.add(sqs.receiveMessage(receiveMessageRequest3).getMessages().get(0));
        */
		}
        catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it " +
                    "to Amazon SQS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } 
        catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered " +
                    "a serious internal problem while trying to communicate with SQS, such as not " +
                    "being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
	} // sendMessage
    
    public static void deleteMessage(AmazonSQS sqs, String queue, List<Message> messages) {
    	
    	try {
    		 System.out.println("Deleting a message.\n");
             String messageRecieptHandle = messages.get(0).getReceiptHandle();
             sqs.deleteMessage(new DeleteMessageRequest(queue, messageRecieptHandle));
    	}
    	
    	catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it " +
                    "to Amazon SQS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered " +
                    "a serious internal problem while trying to communicate with SQS, such as not " +
                    "being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    } // deleteMessage
    
    public static List<Message> receiveMessage(AmazonSQS sqs, String queue) {
		List<Message> messages = null;
		try {
	        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queue);
	        messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
	        }
        catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it " +
                    "to Amazon SQS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } 
        catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered " +
                    "a serious internal problem while trying to communicate with SQS, such as not " +
                    "being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
		
		return messages;

	} // receiveMessage
    
    private static String getUserDataScript(){
        ArrayList<String> lines = new ArrayList<String>();
        //lines.add("#cloud-boothook");
        lines.add("#! /bin/bash\n");
        lines.add("export AWS_DEFAULT_REGION=us-east-1\n");
        lines.add("aws s3 cp s3://shleem/links.txt ./Files/jameson\n");
        lines.add("aws s3 cp s3://shleem/MrManager.jar MrManager.jar\n");
        lines.add("clear\n");
        lines.add("echo -----MrManager downloaded successfully-----\n");
        //lines.add("java -version\n");
        lines.add("mkdir filesToSend\n");
        lines.add("touch ./filesToSend/MrManagerOutput.html\n");
        lines.add("ls\n");
        lines.add("java -jar MrManager.jar\n");
        String str = new String(Base64.encode(join(lines, "\n").getBytes()));
        return str;
    }

    private static String join(Collection<String> s, String delimiter) {
        StringBuilder builder = new StringBuilder();
        Iterator<String> iter = s.iterator();
        while (iter.hasNext()) {
            builder.append(iter.next());
            if (!iter.hasNext()) {
                break;
            }
            builder.append(delimiter);
        }
        return builder.toString();
    }
} // Local
