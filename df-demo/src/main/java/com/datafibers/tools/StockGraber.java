package com.datafibers.tools;

import com.datafibers.conf.ConfigApp;
import org.codehaus.jackson.map.ObjectMapper;
import yahoofinance.Stock;
import yahoofinance.YahooFinance;
import java.io.IOException;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.file.FileSystem;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;


/**
 * This is a tool to fetch stock data and dump it to a path in real time
 * This is used for DF demo purpose.
 */
public class StockGraber extends AbstractVerticle {

    public static String FILE_PATH;
    private ObjectMapper mapper;

    public static void main(String [] args) {

        ConfigApp.getAppConfig("sg.app.property");

        if(args.length != 0) {
            FILE_PATH = args[0];

        } else FILE_PATH = ConfigApp.getStockStageDir();

        System.out.println("INFO: Staging file at " + FILE_PATH);

        Runner.runExample(StockGraber.class);
    }

    public void start() {

        //HttpClient httpClient = vertx.createHttpClient(new HttpClientOptions());
        FileSystem fs = vertx.fileSystem();
        getJsonFile(ConfigApp.getStockList());

    }

    public void getJsonFile(String[] symbols) {

        mapper = new org.codehaus.jackson.map.ObjectMapper();

        vertx.setPeriodic(ConfigApp.getPeriodic(), id -> {

            try {
                //create a new file end with .ignore
                String fileName = new SimpleDateFormat("'stock_'yyyyMMddhhmmss'.json.ignore'").format(new Date());
                String postFileName = fileName.replaceAll(".ignore",""); //This is used at post processing
                String jsonStr;

                Path stagFile = Paths.get(FILE_PATH, fileName);
                Path postStageFile = Paths.get(FILE_PATH, postFileName);

                if(!Files.exists(stagFile)) Files.createFile(stagFile);

                Map<String, Stock> stocks = YahooFinance.get(symbols); // single request
                for(String symbol : stocks.keySet()) {
                    Stock stock = stocks.get(symbol);
                    jsonStr = mapper.writeValueAsString(stock) + System.getProperty("line.separator");
                    Files.write(stagFile, jsonStr.getBytes(), StandardOpenOption.APPEND);
                }

                //Rename by removing .ignore so that DF can process it
                Files.move(stagFile, postStageFile, REPLACE_EXISTING);

            } catch (IOException ioe) {
                ioe.printStackTrace();
            }

        });

    }

}
