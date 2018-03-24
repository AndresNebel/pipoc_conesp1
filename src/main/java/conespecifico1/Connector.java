package conespecifico1;


import static java.lang.System.getenv;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class Connector implements ServletContextListener {
	private Thread myThread = null;
	
	public void contextInitialized(ServletContextEvent sce) {
		if ((myThread == null) || (!myThread.isAlive())) {
            myThread =  new Thread(new IncomingMsgProcess(), "IdleConnectionKeepAlive");
            myThread.start();
        }
    }

    public void contextDestroyed(ServletContextEvent sce){
        try {           
            myThread.interrupt();
        } catch (Exception ex) {}
    }
    
    public static boolean isEmpty(String str) {
  		return str == null || str.trim().length() == 0;
  	}
    
    class IncomingMsgProcess implements Runnable {

		@Override
		public void run() {
			System.out.println("Conector C1: Inicializando Msg Endpoint..");
			
			ConnectionFactory factory = new ConnectionFactory();
			String hostRabbit = getenv("OPENSHIFT_RABBITMQ_SERVICE_HOST");			
			factory.setHost(hostRabbit);
			
			Connection connection;
			try {
				connection = factory.newConnection();
				Channel channel = connection.createChannel();
				channel.queueDeclare("conespecifico1", false, false, false, null);
				
				Consumer consumer = new DefaultConsumer(channel) {
				  @Override
				  public void handleDelivery(String consumerTag, Envelope envelope, 
						  						AMQP.BasicProperties prop, byte[] body) 
						  							throws IOException {
				      
					    String message = new String(body, "UTF-8");
					    sendMessageToC1(message);					    
				  }
				};
				
				channel.basicConsume("conespecifico1", true, consumer);				
				
				System.out.println("Conector C1: Todo listo. Esperando pedidos...");	
				
			} catch (IOException | TimeoutException e) {					
				e.printStackTrace();
			}
		}   
		
		public void sendMessageToC1(String message) {
			HttpPost req = new HttpPost(getC1URL()); 
			HttpClient httpClient = HttpClients.createDefault();
			
			try {
				req.setEntity(new StringEntity(message));
				req.setHeader("Content-Type", "application/json");
				httpClient.execute(req);			
			} catch (Exception e) {
				e.printStackTrace();
			}		
		}
		
		public String getC1URL() {
			String resourcePath = "/sistemac1/pipoc/receive";
			String baseUrl = "";
			
			if (!isEmpty(getenv("SISTEMAC1_SERVICE_HOST")) && !isEmpty(getenv("SISTEMAC1_SERVICE_PORT")))
				baseUrl = "http://" + getenv("SISTEMAC1_SERVICE_HOST") + ":" + System.getenv("SISTEMAC1_SERVICE_PORT"); 
					
			return baseUrl + resourcePath;
		}
		
    }

}
