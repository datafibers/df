package io.example.vertx.client;

import io.example.vertx.util.Runner;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileProps;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.streams.Pump;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;

public class StreamingClient extends AbstractVerticle {


	static String FILE_NAME = null;
	static String FILE_TOPIC = null;
	static String META_TOPIC = "metadata"; //use default value for marking handshake
	static String TRANS_MODE = null;
	static String SERVER_ADDR = "localhost";
	static int SERVER_PORT = 8998;
	static int RES_SUCCESS = 202;

	public static void main(String[] args) {

		if (null == args[0] || args[0] == "" || args[1] == null || args[1] == ""
				|| null == args[2] || args[2] == "" || args[2] == null || args[2] == "") {
			System.err.println("Usage: javac io.vertx.example.http.client.StreamingClient " +
					"<PATH_TO_INPUT_FILE_WITH_FILE_NAME> <FILE_TOPIC> <TRANS_MODE:-s|-b>");
			System.exit(0);
		}

		FILE_NAME = args[0];
		FILE_TOPIC = args[1];
		TRANS_MODE = args[2];
		Runner.runExample(StreamingClient.class);
	}

	public void start() {

		HttpClient httpClient = vertx.createHttpClient(new HttpClientOptions());
		FileSystem fs = vertx.fileSystem();
		handshake(httpClient, fs);

	}

	public void handshake(HttpClient hc, FileSystem fs) {

		HttpClientRequest request = hc.put(SERVER_PORT, SERVER_ADDR, "", resp -> {
			System.out.println("Response: Hand Shake Status Code - " + resp.statusCode());
			System.out.println("Response: Hand Shake Status Message - " + resp.statusMessage());
			if (resp.statusCode() == RES_SUCCESS) {
 				System.out.println("Response: Hand Shake Status - SUCCESSFUL!");
 				streamfile(hc, fs);
 			}
 			else System.out.println("Response: Hand Shake Status - FAILED!");
		});
		    request.end(setMetaData(FILE_NAME));
 	}

	public void streamfile(HttpClient hc, FileSystem fs) {

		HttpClientRequest request = hc.put(SERVER_PORT, SERVER_ADDR, "", resp -> {
			System.out.println("Response: File Streaming Status Code - " + resp.statusCode());
			System.out.println("Response: File Streaming Status Message - " + resp.statusMessage());
		});

		fs.props(FILE_NAME, ares -> {
			FileProps props = ares.result();
			request.headers().set("content-length", String.valueOf(props.size()));
			fs.open(FILE_NAME, new OpenOptions(), ares2 -> {
				AsyncFile file = ares2.result();
				Pump pump = Pump.pump(file, request);
				file.endHandler(v -> {
					request.end();
				});
				pump.start();
			});
		});
	}

	public String setMetaData(String finlename) {

		String jsonstring = null;
		try {
			BasicFileAttributes attr = Files.readAttributes(Paths.get(finlename), BasicFileAttributes.class);
			jsonstring = "{\"" + "file_length" + "\":\"" + String.valueOf(attr.size()) + "\"," +
						 "\"" + "creation_date" + "\":\"" + String.valueOf(attr.creationTime()) + "\"," +
						 "\"" + "file_name" + "\":\"" + FILE_NAME + "\"," +
						 "\"" + "topic" + "\":\"" + META_TOPIC + "\"," +
						 "\"" + "mode"+ "\":\"" + TRANS_MODE + "\"}";
			System.out.println("INFO: Metadata Json String is - " + jsonstring);

		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return jsonstring;
	}
}
