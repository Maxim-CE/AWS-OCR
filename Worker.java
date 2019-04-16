import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.List;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.asprise.ocr.Ocr;

public class Worker {
	public static void main(String[] args) {
		System.out.println(" --- Worker started --- \n");	
		String imageUrl = new String();
		String imageFile = new String();
		String imageText = new String();
		try {
			AmazonSQS sqs = AmazonSQSClientBuilder.standard()
	                .withRegion("us-east-1")
	                .build();			
			String newImageTask = sqs.getQueueUrl("newImageTask").getQueueUrl();
			String doneImageTask = sqs.getQueueUrl("doneImageTask").getQueueUrl();
			boolean messagesPolling = true;
			while (messagesPolling) {
				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(newImageTask);
				List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
				System.out.println("Messages received from newImageTask queue!");
				for (Message message : messages) {
					imageUrl = message.getBody();
					System.out.println("Worker is deleting the message from newImageTask queue: " + imageUrl);
					sqs.deleteMessage(new DeleteMessageRequest(newImageTask, message.getReceiptHandle()));
					System.out.println("Downloading image from url: " + imageUrl);
					imageFile = downloadImage(message.getBody());
					try {
						System.out.println("Launching OCR on image");
						Ocr.setUp();
						Ocr ocr = new Ocr();
						ocr.startEngine("eng", Ocr.SPEED_FASTEST);
						imageText = ocr.recognize(new File[] {new File(imageFile)}, Ocr.RECOGNIZE_TYPE_ALL, Ocr.OUTPUT_FORMAT_PLAINTEXT);
						ocr.stopEngine();
					}
					catch(Exception ex){
						System.out.println("OCR failure");
						imageText = "OCR FAILED!";
					}
					 				
					System.out.println("Worker is sending the message to doneImageTask queue");
					String messageBody = imageUrl + " " + imageText;
					SendMessageRequest sendMessageRequest = new SendMessageRequest()
					        .withQueueUrl(doneImageTask)
					        .withMessageBody(messageBody);
					sqs.sendMessage(sendMessageRequest);

				}
				if (messages.size() == 0) {
					messagesPolling = false;
				}
			}
			System.out.println(" --- Worker finished --- \n");
		} 
		catch (Exception ex) {
			System.out.println("Worker Exception!");
			System.out.println(ex.getMessage());
		}
	}


	public static String downloadImage(String imageUrl) {
		String imageFile = "";
		try {
			URL url = new URL(imageUrl);
			int i = imageUrl.lastIndexOf('.');
			if (i > 0) {
				imageFile = "image." + imageUrl.substring(i + 1);
			}
			InputStream is = url.openStream();
			OutputStream os = new FileOutputStream(imageFile);
			byte[] b = new byte[2048];
			int length;
			while ((length = is.read(b)) != -1) {
				os.write(b, 0, length);
			}
			is.close();
			os.close();
		} catch (Exception ex) {
			System.out.println(ex.getMessage());
		}
		return imageFile;
	}
}
