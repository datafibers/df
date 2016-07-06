package io.example.vertx.server;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.example.conf.ConfigApp;
import io.example.producers.KafkaStreamProducer;
import io.example.vertx.util.Runner;
import io.example.conf.ConstantApp;
import io.example.vertx.util.ServerFunc;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;

public class StreamingServer extends AbstractVerticle {

	  public static void main(String[] args)
	  {

		  Runner.runExample(StreamingServer.class);

	  }

	  static Pattern p = Pattern.compile(Pattern.quote(ConstantApp.JSON_PATTERN_L) + "(.*?)" + Pattern.quote(ConstantApp.JSON_PATTERN_R));

	  /* (non-Javadoc)
	   * @see io.vertx.core.AbstractVerticle#start()
	   */
	@Override
	  public void start() throws Exception {
		System.out.println("INFO: Server started at - localhost:8998");
		vertx.createHttpServer().requestHandler(new Handler<HttpServerRequest>() {
	    	
	    	long byteswritten = 0;
	    	
			KafkaStreamProducer ksp = new KafkaStreamProducer();
			
			List<String> results = new ArrayList<String>();
			String extraBytes = null;
			
	    	int start = -1;
	    	int end = -1;

			@Override
			public void handle(HttpServerRequest request) {
				request.handler(new Handler<Buffer>(){

					@Override
					public void handle(Buffer buffer) 
					{
						byteswritten += buffer.length();
						String inputString = extraBytes == null ? buffer.toString(): extraBytes + buffer.toString();
						extraBytes = null;
						//using string patterns

						//System.out.println("INFO: Message received from client - " + inputString);

						Matcher m = p.matcher(inputString);
						//set request content-length header
                        MultiMap headers = request.headers();
                        headers.set("content-length", String.valueOf(byteswritten));

						ServerFunc.printToConsole("INFO", headers);

						while (m.find()) {
							if(start < 0){
								if(m.start() > 0)
									start = m.start();
							}
							try{
								if(headers.get("DF_TYPE").equalsIgnoreCase(ConstantApp.DF_TYPE_MEATA)) {
									ksp.sendMessages(ConstantApp.META_TOPIC, m.group());
									ServerFunc.printToConsole("INFO", "Metadata => KAFKA @" + m.group());
								}
								if(headers.get("DF_TYPE").equalsIgnoreCase(ConstantApp.DF_TYPE_PAYLOAD)) {

									switch (headers.get("DF_MODE")) {
										case ConstantApp.DF_MODE_STREAM_KAFKA:
											ksp.sendMessages(headers.get("DF_TOPIC"), m.group());
											ServerFunc.printToConsole("INFO", "Data => KAFKA in streaming");
											break;
										case ConstantApp.DF_MODE_STREAM_HDFS:
											ServerFunc.printToConsole("INFO", "Data => HDFS in streaming");
											break;
										case ConstantApp.DF_MODE_BATCH_HDFS:
											ServerFunc.printToConsole("INFO", "Data => HDFS in batching");
											break;
										case ConstantApp.DF_MODE_BATCH_HIVE:
											ServerFunc.printToConsole("INFO", "Data => HIVE in batching");
											break;
										default:
											ServerFunc.printToConsole("INFO", "Data => NULL!");
											break;
									}
								}

							}catch(Exception ex){
								ex.printStackTrace();
								ServerFunc.printToConsole("ERROR", "Sending data exception!", Boolean.TRUE);
							}
							end = m.start() + m.group().length();
						}//while

						if(end >= 0 && end < inputString.length()){
							extraBytes = inputString.substring(end, inputString.length());
						}

						/*---------------------jackson parser
						try{
								parser = jsonfactory.createJsonParser(buffer.toString());
								JsonToken jt = null;
								JsonToken intjt = null;
								
								while (parser.nextToken() != JsonToken.END_ARRAY) 
								{
									jt = parser.getCurrentToken();
									if(jt == JsonToken.START_OBJECT){
										while(parser.nextToken() != JsonToken.END_OBJECT){
											intjt = parser.getCurrentToken();
											if(intjt == JsonToken.FIELD_NAME){
												System.out.println(":"+intjt.asString());
											}
										}
									}
								//	System.out.println("==> "+parser.nextToken());
								//	System.out.println(parser.getCurrentName());

								}

						}catch(Exception ex){
							ex.printStackTrace();
						}
						*///-------ends---jackson parser
					}

				});//request.handler

				request.endHandler(new Handler<Void>() {
					@Override
					public void handle(Void event) {
						System.out.println("INFO: request.endHandler is called");
						try {
							request.response().setStatusCode(202).setStatusMessage("bytes written " + byteswritten);
							request.response().end();
							extraBytes = null;
							results = new ArrayList<String>();
							byteswritten = 0;

							//Here, keep a copy of data to HDFS and write the metadata to metadata database
						} catch (Exception e) {
							e.printStackTrace();
						}finally{
							//ksp.closeProducer();
							//reset
							extraBytes = null;
							results = new ArrayList<String>();
							byteswritten = 0;
						}
					}//handle
				});
			}
	    }).listen(ConfigApp.getServerPort());
	  }
}
