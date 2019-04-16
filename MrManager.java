package com.amazonaws.samples;

import java.io.BufferedReader;
//import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
//import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
//import com.amazonaws.auth.AWSCredentialsProvider;
//import com.amazonaws.auth.AWSStaticCredentialsProvider;
//import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.util.Base64;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;

public class MrManager {
	
	private static AmazonEC2 ec2 = null;
	private static List<Instance> instances = null;
	private static int deleteCounter = 1;
	
	public static void main(String[] args) {
		System.out.println("----------------Starting MrManager----------------\n");
		//AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials()); //Comment for use in cloud.
//Get access to AmazonSQS.
        AmazonSQS sqs = AmazonSQSClientBuilder.standard()
               // .withCredentials(credentialsProvider) //Comment for use in cloud.
                .withRegion("us-east-1")
                .build();
        String newTask = sqs.getQueueUrl("newTask").getQueueUrl(); //Get access to SQS to specific queue.
        String newImageTask = sqs.getQueueUrl("newImageTask").getQueueUrl();
        String doneImageTask = sqs.getQueueUrl("doneImageTask").getQueueUrl();
        String doneTask = sqs.getQueueUrl("doneTask").getQueueUrl();
        
//Get access to AmazonS3.
        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
        		//.withCredentials(credentialsProvider) //Comment for use in cloud.
        		.withRegion("us-east-1")
        		.build();
        String filePath = "./filesToSend";
        String urlsLocation = "shleem-MrManagerOutput.html";
        
//Get bucket and file name of URL's.         	
        List<Message> fileLoc = receiveMessage(sqs, newTask);
        String fileLocation = fileLoc.get(0).getBody();
        deleteMessage(sqs, newTask, fileLoc);
        int indexOf = fileLocation.indexOf(" ");
        System.out.println("This is the path to file: " + fileLocation + "\n");
        String bucketName = fileLocation.substring(0, 6);
        String fileName = fileLocation.substring(7, indexOf);
        System.out.println("This is the file name: " + fileName + "\n");
        String imagesPerWorker = fileLocation.substring(indexOf + 1, fileLocation.length());
        System.out.println("This is the images per worker: " + imagesPerWorker + "\n");
        int imagesPerWorkerInt = Integer.parseInt(imagesPerWorker);
        int amountOfWorkers = 1;
        
//Downloading URL file from S3 bucketName and sending them to sqs as messages.
        int sentMessagesCounter = 0; //Counter for later use, so the manager will wait for all workers to finsh their job.
        List<String> test = downloadObject(s3, bucketName, fileName);
		for(String msg : test) {
            System.out.println("Sending message: " + msg);
            sendMessage(sqs, newImageTask, msg);
            //sendMessage(sqs, doneImageTask, msg); //For testing deleteMessage
            sentMessagesCounter++;
            if(sentMessagesCounter % imagesPerWorkerInt == 0)
            	amountOfWorkers++;
        }
//Creating the workers.
		createWorkers(amountOfWorkers);//Enable when converting to JAR---------------------------------------------------------------
		System.out.println(amountOfWorkers + " Workers launched successfully.\n");
		
//Blocking, waiting for workers to finish.
		List<Message> messages = null;
		String htmlFile = "";
		String url;
		String ocrResult;
		String firstMsg;
		String htmlStart = "<!DOCTYPE html>\n<html>\n<body>\n\n";
		String htmlEnd ="</body>\n</html>\n";
		htmlFile += htmlStart;
		while(!(messages = receiveMessage(sqs, doneImageTask)).isEmpty() | (sentMessagesCounter > 0)) {
			if(sentMessagesCounter > 0 && messages.isEmpty())
				continue;
			else {
				for(Message message : messages) {
					//htmlFile = htmlFile + "\n" + messages.get(0).getBody();
					sentMessagesCounter--;
					//firstMsg = messages.get(0).getBody();
					firstMsg = message.getBody();
					indexOf = firstMsg.indexOf(" ");
					url = firstMsg.substring(0, indexOf);
					ocrResult = firstMsg.substring(indexOf + 1, firstMsg.length());
					htmlFile += "<img src=" + url +">\n\n";
					htmlFile += "<p>" + ocrResult + "</p>\n\n";
					//System.out.println("url: " + url + "\nocrResult: " + ocrResult + "\n");
				}
				deleteMessage(sqs, doneImageTask, messages);
			}
		}
		htmlFile += htmlEnd;
		//System.out.println(htmlFile);
		
		/*
		File file = new File ("./filesToSend/MrManagerOutput.html");
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(file));
			bw.write(htmlFile);
			bw.close();
		} 
		catch (Exception ex) {
			System.out.println(ex.getMessage());
		}
		System.out.println("Finished to create HTML file\n");
		*/
		
		PrintWriter out;
		try {
			out = new PrintWriter("./filesToSend/MrManagerOutput.html"); 
			out.println(htmlFile);
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		System.out.println("\nsentMessagesCounter: " + sentMessagesCounter + "\n");
		//filePath = "filesToSend";
		//bucketName = "shleem"; //For testing
		uploadFile(filePath, bucketName, s3);
		System.out.println("MrManagerOutput.html uploaded to " + bucketName + " successfully.\n");
		sendMessage(sqs, doneTask, urlsLocation);
		Terminator();
    } // Main
	
	public static void sendMessage(AmazonSQS sqs, String queue, String message) {
		try {
        // Send a message
        System.out.println("Sending a message to MyQueue.\n");
       // sqs.sendMessage(new SendMessageRequest(myQueueUrl, "This is my message text."));
        sqs.sendMessage(new SendMessageRequest(queue, message));
        // Receive messages
        System.out.println("Receiving messages from MyQueue.\n");
        
        /*							Working like this
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

	}
	
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
	
	public static void deleteMessage(AmazonSQS sqs, String queue, List<Message> messages) {
    	
    	try {
    		 System.out.println(deleteCounter + " ");
    		 deleteCounter++;
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
    	
    public static List<String> downloadObject(AmazonS3 s3, String bucketName, String fileName) {
    	
        List<String> ret = new ArrayList<String>();
	    try {
	    	System.out.println("Downloading an object");
	        //S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
	    	S3Object object = s3.getObject(new GetObjectRequest(bucketName, fileName));
	        System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
	        //displayTextInputStream(object.getObjectContent());
	        
	        
	        

            BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
            String line;

            while((line = reader.readLine()) != null) {
            // can copy the content locally as well
            // using a buffered writer
            	ret.add(line);
            	//System.out.println(line);
          
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
	    } catch (IOException e) {
			e.printStackTrace();
		}
	    
	    return ret;
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
    	System.out.println("---File uploaded---\n");
    } // uploadFile
    
    public static void createWorkers(int numOfWorkers) {
    	ec2 = AmazonEC2ClientBuilder.standard()
    			.withRegion("us-east-1") // Possible to use east-1 only, default.
    			.build();

    	IamInstanceProfileSpecification iamProfile = new IamInstanceProfileSpecification();
    	iamProfile.setName("Worker");

    	try {
    		// Basic 32-bit Amazon Linux AMI 1.0 (AMI Id: ami-08728661)
    		RunInstancesRequest request = new RunInstancesRequest("ami-1853ac65", numOfWorkers, numOfWorkers);
    		request.setInstanceType(InstanceType.T2Micro.toString());
    		request.withKeyName("secKey");
    		request.setIamInstanceProfile(iamProfile);
    		request.setUserData(getUserDataScript());
    		instances = ec2.runInstances(request).getReservation().getInstances();
    		System.out.println("Launch instances: " + instances);
    		/*RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
          	runInstancesRequest.withImageId("ami-1853ac65")			//The ID of the AMI. For a list of public AMIs provided by Amazon, see Amazon Machine Images.
              .withInstanceType("t2.micro")							//An instance type that is compatible with the specified AMI.
              .withMinCount(1)
              .withMaxCount(1)
              .withKeyName("my-key-pair")
              .withSecurityGroups("my-security-group"); */
    	} catch (AmazonServiceException ase) {
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
    
    private static String getUserDataScript(){ 
        ArrayList<String> lines = new ArrayList<String>();
        //lines.add("#cloud-boothook");
        lines.add("#! /bin/bash\n");
        lines.add("echo ---The worker has woken up---\n");
        lines.add("echo ---Downloading Worker.jar---\n");
        //lines.add("export AWS_DEFAULT_REGION=us-east-1\n");
        //lines.add("aws s3 cp s3://shleem/links.txt ./Files/jameson\n");
        lines.add("aws s3 cp s3://shleem/Worker.jar Worker.jar\n");
        //lines.add("java -version\n");
        //lines.add("cat ./Files/jameson\n");
        lines.add("echo ---Downloading complete---\n");
        lines.add("echo ---Worker initialization in progress---\n");
        lines.add("java -jar Worker.jar\n");
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
      
}// MrManager


