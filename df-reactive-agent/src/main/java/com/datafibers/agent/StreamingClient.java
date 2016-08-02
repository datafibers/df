package com.datafibers.agent;

import com.datafibers.util.AgentConstant;
import com.datafibers.util.FileUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.datafibers.util.MetaDataPOJO;
import com.datafibers.util.Runner;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Handler;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileProps;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.streams.Pump;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class StreamingClient extends AbstractVerticle {

	public static void main(String[] args) {

		if (null == args[0] || args[0] == "" || args[1] == null || args[1] == ""
				|| null == args[2] || args[2] == "" || args[2] == null || args[2] == "") {
			System.err.println("Usage: javac io.vertx.example.http.client.StreamingClient " +
					"<PATH_TO_INPUT_FILE_WITH_FILE_NAME> <FILE_TOPIC> <TRANS_MODE:-s|-b> <FILTER_TYPE> <TRANS_TYPE>");
			System.exit(0);
		}

		AgentConstant.FILE_NAME = args[0];
		AgentConstant.FILE_TOPIC = args[1];
		AgentConstant.TRANS_MODE = args[2];
		
		if (args.length > 3) {
			AgentConstant.FILTER_TYPE = args[3];
		}
		
		if (args.length > 4) {
			AgentConstant.DATA_TRANS = args[4];
		}
		
		Runner.runExample(StreamingClient.class);
	}

	public void start() {

		HttpClient httpClient = vertx.createHttpClient(new HttpClientOptions());
		FileSystem fs = vertx.fileSystem();
		handshake(httpClient, fs);
	}

	public void handshake(HttpClient hc, FileSystem fs) {

		HttpClientRequest request = hc.put(AgentConstant.SERVER_PORT, AgentConstant.SERVER_ADDR, "", resp -> {
			System.out.println("Response: Hand Shake Status Code - " + resp.statusCode());
			System.out.println("Response: Hand Shake Status Message - " + resp.statusMessage());
			if (resp.statusCode() == AgentConstant.RES_SUCCESS) {
 				System.out.println("Response: Hand Shake Status - SUCCESSFUL!");

				//check if it is file/folder processing
				if(Files.isDirectory(Paths.get(AgentConstant.FILE_NAME))) {
					streamFilesDir(hc, fs);
				} else streamFile(hc, fs);
 			}
 			else System.out.println("Response: Hand Shake Status - FAILED!");
		});
		request.headers().add("DF_PROTOCOL","REGISTER");
		request.headers().add("DF_MODE", AgentConstant.TRANS_MODE);
		request.headers().add("DF_TYPE", "META");
		request.headers().add("DF_TOPIC", AgentConstant.META_TOPIC);
		request.headers().add("DF_FILENAME", AgentConstant.FILE_NAME);
		request.headers().add("DF_FILTER_TYPE", AgentConstant.FILTER_TYPE);
		request.headers().add("DF_DATA_TRANS", AgentConstant.DATA_TRANS);
		
		request.end(setMetaData(AgentConstant.FILE_NAME));
 	}

	public String setMetaData(String fileName) {


		MetaDataPOJO md = new MetaDataPOJO();
		ObjectMapper mapperObj = new ObjectMapper();

		try {
			BasicFileAttributes attr = Files.readAttributes(Paths.get(fileName), BasicFileAttributes.class);
			md.setFileLength(attr.size());
			md.setCreateDate(String.valueOf(attr.creationTime()));
			md.setFileName(AgentConstant.FILE_NAME);
			md.setTopic(AgentConstant.META_TOPIC);
			md.setMode(AgentConstant.TRANS_MODE);
			
			return mapperObj.writeValueAsString(md);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;

	}

	/**
	 * This is applied to folder processing only. The daemon will watch the folder changes and process new files
	 * Once the files are processed (streamed), the file is archived to the archive folder
	 * @param hc
	 * @param fs
     */
	public void streamFilesDir(HttpClient hc, FileSystem fs) {
		Path directoryPath = Paths.get(AgentConstant.FILE_NAME);

		//This is where unblocking while true loop
		long timerID = vertx.setPeriodic(AgentConstant.FILE_WATCHER_PERIODIC, id -> {
			fs.readDir(directoryPath.toString(), AgentConstant.FILE_FILTER_REGX, new AsyncResultHandler<List<String>>() {
				@Override
				public void handle(AsyncResult<List<String>> asyncResult) {
					if (asyncResult.failed()) {
						asyncResult.cause();
					} else {
						for (String processingFile : asyncResult.result()) {
							//Rename files before processing
							try {
								Files.move(Paths.get(processingFile),
										Paths.get(processingFile + AgentConstant.PROCESSING_FILES_POSTFIX),
										REPLACE_EXISTING);
							} catch (IOException e) {
								throw new RuntimeException(e);
							}
							HttpClientRequest request = hc.put(AgentConstant.SERVER_PORT, AgentConstant.SERVER_ADDR, "", resp -> {
								System.out.println("Response: File Streaming Status Code - " + resp.statusCode());
								System.out.println("Response: File Streaming Status Message - " + resp.statusMessage());

								try {
									Files.move(Paths.get(processingFile.toString() + AgentConstant.PROCESSING_FILES_POSTFIX),
											Paths.get(processingFile.toString() + AgentConstant.PROCESSED_FILES_POSTFIX),
											REPLACE_EXISTING);

								} catch (IOException ioe) {
									ioe.printStackTrace();
								}
							});

							fs.props(Paths.get(processingFile + AgentConstant.PROCESSING_FILES_POSTFIX).toString(),
									ares -> {
										FileProps props = ares.result();
										request.headers().set("content-length", String.valueOf(props.size()));
										request.headers().set("DF_MODE", AgentConstant.TRANS_MODE);
										request.headers().set("DF_TYPE", "PAYLOAD");
										request.headers().add("DF_TOPIC", AgentConstant.FILE_TOPIC);
										request.headers().add("DF_FILENAME", processingFile);
										fs.open(Paths.get(processingFile + AgentConstant.PROCESSING_FILES_POSTFIX).toString(),
												new OpenOptions(), ares2 -> {
													AsyncFile file = ares2.result();
													Pump pump = Pump.pump(file, request);
													file.endHandler(v -> {
														request.end();
													});
													pump.start();
												});
									});

						} //end for
					}

				}//end handler
			});
		} //end of while
		);
	}

	/**
	 * This is applied to single file processing only. The daemon will process the specified single file.
	 * Once the files are processed (streamed), the file is archived to the archive folder
	 * @param hc
	 * @param fs
	 */
	public void streamFile(HttpClient hc, FileSystem fs) {
		Path singleFilePath = Paths.get(AgentConstant.FILE_NAME);
		try {
			Files.move(singleFilePath,
					Paths.get(singleFilePath + AgentConstant.PROCESSING_FILES_POSTFIX),
					REPLACE_EXISTING);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		HttpClientRequest request = hc.put(AgentConstant.SERVER_PORT, AgentConstant.SERVER_ADDR, "", resp -> {
			System.out.println("Response: File Streaming Status Code - " + resp.statusCode());
			System.out.println("Response: File Streaming Status Message - " + resp.statusMessage());

			try {
				Files.move(Paths.get(singleFilePath.toString() + AgentConstant.PROCESSING_FILES_POSTFIX),
						Paths.get(singleFilePath.toString() + AgentConstant.PROCESSED_FILES_POSTFIX),
						REPLACE_EXISTING);

			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		});

		fs.props(Paths.get(singleFilePath + AgentConstant.PROCESSING_FILES_POSTFIX).toString(),
				ares -> {
					FileProps props = ares.result();
					request.headers().set("content-length", String.valueOf(props.size()));
					request.headers().set("DF_MODE", AgentConstant.TRANS_MODE);
					request.headers().set("DF_TYPE", "PAYLOAD");
					request.headers().add("DF_TOPIC", AgentConstant.FILE_TOPIC);
					request.headers().add("DF_FILENAME", singleFilePath.toString());
					fs.open(Paths.get(singleFilePath + AgentConstant.PROCESSING_FILES_POSTFIX).toString(),
							new OpenOptions(), ares2 -> {
								AsyncFile file = ares2.result();
								Pump pump = Pump.pump(file, request);
								file.endHandler(v -> {
									request.end();
								});
								pump.start();
							});
				});

	}

}
