package com.datafibers.agent;

import com.datafibers.util.AgentConstant;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.datafibers.util.MetaDataPOJO;
import com.datafibers.util.Runner;
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

	public static void main(String[] args) {

		if (null == args[0] || args[0] == "" || args[1] == null || args[1] == ""
				|| null == args[2] || args[2] == "" || args[2] == null || args[2] == "") {
			System.err.println("Usage: javac io.vertx.example.http.client.StreamingClient " +
					"<PATH_TO_INPUT_FILE_WITH_FILE_NAME> <FILE_TOPIC> <TRANS_MODE:-s|-b>");
			System.exit(0);
		}

		AgentConstant.FILE_NAME = args[0];
		AgentConstant.FILE_TOPIC = args[1];
		AgentConstant.TRANS_MODE = args[2];
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
 				streamfile(hc, fs);
 			}
 			else System.out.println("Response: Hand Shake Status - FAILED!");
		});
		request.headers().add("DF_PROTOCOL","REGISTER");
		request.headers().add("DF_MODE", AgentConstant.TRANS_MODE);
		request.headers().add("DF_TYPE", "META");
		request.headers().add("DF_TOPIC", AgentConstant.META_TOPIC);
		request.headers().add("DF_FILENAME", AgentConstant.FILE_NAME);
		request.end(setMetaData(AgentConstant.FILE_NAME));
 	}

	public void streamfile(HttpClient hc, FileSystem fs) {

		HttpClientRequest request = hc.put(AgentConstant.SERVER_PORT, AgentConstant.SERVER_ADDR, "", resp -> {
			System.out.println("Response: File Streaming Status Code - " + resp.statusCode());
			System.out.println("Response: File Streaming Status Message - " + resp.statusMessage());
		});

		fs.props(AgentConstant.FILE_NAME, ares -> {
			FileProps props = ares.result();
			request.headers().set("content-length", String.valueOf(props.size()));
			request.headers().set("DF_MODE", AgentConstant.TRANS_MODE);
			request.headers().set("DF_TYPE", "PAYLOAD");
			request.headers().add("DF_TOPIC", AgentConstant.FILE_TOPIC);
			request.headers().add("DF_FILENAME", AgentConstant.FILE_NAME);
			fs.open(AgentConstant.FILE_NAME, new OpenOptions(), ares2 -> {
				AsyncFile file = ares2.result();
				Pump pump = Pump.pump(file, request);
				file.endHandler(v -> {
					request.end();
				});
				pump.start();
			});
		});
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
}
