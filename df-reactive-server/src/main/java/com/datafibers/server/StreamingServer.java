package com.datafibers.server;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import com.datafibers.conf.ConfigApp;
import com.datafibers.util.MsgFilter;
import com.datafibers.util.Runner;
import com.datafibers.util.ServerFunc;
import com.datafibers.producers.HDFSStreamProducer;
import com.datafibers.producers.KafkaStreamProducer;
import com.datafibers.conf.ConstantApp;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;

import static java.lang.System.out;

public class StreamingServer extends AbstractVerticle {

	  public static void main(String[] args)
	  {

		  Runner.runExample(StreamingServer.class);

	  }

	  /* (non-Javadoc)
	   * @see io.vertx.core.AbstractVerticle#start()
	   */
	  @Override
	  public void start() throws Exception {
		out.println("INFO: Server started at - localhost:8998");
		out.println("INFO: Server debug mode - " + ConfigApp.getServerDebugMode().toString().toUpperCase());

		vertx.createHttpServer().requestHandler(new Handler<HttpServerRequest>() {
	    	
	    	long byteswritten = 0;
	    	
			KafkaStreamProducer ksp = new KafkaStreamProducer();

			List<String> results = new ArrayList<String>();
			String extraBytes = null;
			
	    	int start = -1;
	    	int end = -1;
			Matcher m = null;

			@Override
			public void handle(HttpServerRequest request) {
				request.handler(new Handler<Buffer>(){

					@Override
					public void handle(Buffer buffer)
					{
						byteswritten += buffer.length();
						String inputString = extraBytes == null ? buffer.toString(): extraBytes + buffer.toString();
						extraBytes = null;

						//set request content-length header
                        MultiMap headers = request.headers();
                        headers.set("content-length", String.valueOf(byteswritten));

						ServerFunc.printToConsole("INFO", headers);

						/*
						 * Process metadata and authentication later
						 */
						if(headers.get("DF_TYPE").equalsIgnoreCase(ConstantApp.DF_TYPE_MEATA)) {
							try {
								//send metadata to kafka
								ksp.sendMessages(ConstantApp.META_TOPIC, inputString);

								//decide the message filter
								m = MsgFilter.getPattern(headers.get("DF_FILTER")).matcher(inputString);

								ServerFunc.printToConsole("INFO", "Metadata => KAFKA @" + m.group());
								switch (headers.get("DF_MODE")) {
									case ConstantApp.DF_MODE_STREAM_KAFKA:
										break;
									case ConstantApp.DF_MODE_STREAM_HDFS:
										break;
									case ConstantApp.DF_MODE_BATCH_HDFS:
									case ConstantApp.DF_MODE_BATCH_HIVE:
									default:
										ServerFunc.printToConsole("INFO", "Meta Data => NULL!");
										break;
								}

							} catch (Exception ex) {
								ex.printStackTrace();
								ServerFunc.printToConsole("ERROR", "Sending metadata exception!", Boolean.TRUE);
							}
						} //end if for metadata processing

						/*
						 * Process payload and its request. While loop for message filters
						 */
						if(headers.get("DF_TYPE").equalsIgnoreCase(ConstantApp.DF_TYPE_PAYLOAD)) {
							while (m.find()) {
								if (start < 0) {
									if (m.start() > 0)
										start = m.start();
								}
								try {
										switch (headers.get("DF_MODE")) {
											case ConstantApp.DF_MODE_STREAM_KAFKA:
												ksp.sendMessages(headers.get("DF_TOPIC"), m.group());
												ServerFunc.printToConsole("INFO", "Data => KAFKA in streaming");
												break;
											case ConstantApp.DF_MODE_STREAM_HDFS:
												HDFSStreamProducer.sendMessages(headers.get("DF_FILENAME"), inputString);
												ServerFunc.printToConsole("INFO", "Data => HDFS in streaming");
												break;
											case ConstantApp.DF_MODE_BATCH_HDFS:
												ServerFunc.printToConsole("INFO", "Data => HDFS in batching");
												break;
											case ConstantApp.DF_MODE_BATCH_HIVE:
												ServerFunc.printToConsole("INFO", "Data => HIVE in batching");
												break;
											default:
												ServerFunc.printToConsole("INFO", "Payload Data => NULL!");
												break;
										}

								} catch (Exception ex) {
									ex.printStackTrace();
									ServerFunc.printToConsole("ERROR", "Sending data exception!", Boolean.TRUE);
								}
								end = m.start() + m.group().length();
							}//while
						} //end if for payload processing

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

						ServerFunc.printToConsole("INFO", "Request endHandler is called");

						try {
							//handle specific request
							switch(request.headers().get("DF_TYPE")) {
								case ConstantApp.DF_TYPE_MEATA:
									request.response().setStatusCode(202).setStatusMessage("Handshake OK");
									break;
								case ConstantApp.DF_TYPE_PAYLOAD:
									HDFSStreamProducer.uploadToHDFS(request.headers().get("DF_FILENAME"));
									request.response().setStatusCode(202).setStatusMessage("bytes written " + byteswritten);
									break;
								default:
									ServerFunc.printToConsole("INFO", "Response Data => NULL!");
									break;
							}
							//handle generic response
							request.response().end();
							extraBytes = null;
							results = new ArrayList<String>();
							byteswritten = 0;

							//Here, keep a copy of data to HDFS and write the metadata to metadata database
						} catch (Exception e) {
							e.printStackTrace();
						}finally{
							//ksp.closeProducer();
							//hdfssp.closeProducer();
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
