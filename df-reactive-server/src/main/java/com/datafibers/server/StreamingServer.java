package com.datafibers.server;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.datafibers.conf.ConfigApp;
import com.datafibers.producers.MongoStreamProducer;
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
			MongoStreamProducer msp = new MongoStreamProducer(vertx);

			List<String> results = new ArrayList<String>();
			String extraBytes = null;

	    	int start = -1;
	    	int end = -1;
			Pattern p = null;
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
								//send metadata to kafka or mongodb or both
								if (ConfigApp.getMetaEnabledKafka())
									ksp.sendMessages(ConstantApp.META_TOPIC, inputString);
								if (ConfigApp.getMetaEnabledMongodb())
									msp.sendMessages(ConfigApp.getMetaMongodbName(), inputString);


								//decide the message filter pattern
								//m = MsgFilter.getPattern(headers.get("DF_FILTER")).matcher(inputString);
								p = MsgFilter.getPattern(headers.get("DF_FILTER"));

								ServerFunc.printToConsole("INFO", "Metadata => KAFKA @" + inputString);
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

							m = p.matcher(inputString);
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
												HDFSStreamProducer.sendMessages(headers.get("DF_FILENAME"), m.group());
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
					}

				});//request.handler

				request.endHandler(new Handler<Void>() {
					@Override
					public void handle(Void event) {

						ServerFunc.printToConsole("INFO", "Request endHandler is called");

						//Clean up code
						try {

							//post processing function
							switch(request.headers().get("DF_MODE")) {

								case ConstantApp.DF_MODE_STREAM_KAFKA:
									break;
								case ConstantApp.DF_MODE_STREAM_HDFS:
									//Once streaming is done, upload the whole file to HDFS
									HDFSStreamProducer.uploadToHDFS(request.headers().get("DF_FILENAME"));
									break;
								case ConstantApp.DF_MODE_BATCH_HDFS:
									break;
								case ConstantApp.DF_MODE_BATCH_HIVE:
									break;
								default:
									ServerFunc.printToConsole("INFO", "Payload Data => NULL!");
									break;
							}

							//post process feedback for specific request
							switch(request.headers().get("DF_TYPE")) {
								case ConstantApp.DF_TYPE_MEATA:
									request.response().setStatusCode(202).setStatusMessage("Handshake OK");
									break;
								case ConstantApp.DF_TYPE_PAYLOAD:
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
