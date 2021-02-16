package s3client;

import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;


public class CallClass {

    private static final Logger logger = LogManager.getLogger(CallClass.class.getName());
    private static final Options options;

    static {
        options = new Options();
        options.addOption("login", true, "Select connection for the S3 storage (nonPROD, PROD)")
                .addOption("createBucket", true, "Create new bucket")
                .addOption("uploadFile", false, "Upload new file to an existing bucket (see properties)")
                .addOption("downloadFile", false, "Download a Object with VersionID (see properties)")
                .addOption("deleteBucket", true, "Deleting a bucket with all objects and versions")
                .addOption("listBucketToFile", true, "Lists all objects of a bucket and exports them as file");
    }

    public static void main(String[] args) throws Exception {
        S3LoadClient myS3API = null;

        try {
            //Auswertung der Übergebenen Parameter
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);

            if (args.length <= 0) {
                help();
            } else {
                logger.info("===========================================");
                logger.info("Getting Started with S3 TestClient");
                logger.info("===========================================");
                logger.info("Parsing the parameters!");
            }

            //Load Properties File
            Properties properties = new Properties();

            try (FileReader fileReader = new FileReader("S3Client.properties")) {
                properties.load(fileReader);
            } catch (IOException e) {
                logger.error("Error while loading the file S3Client.properties", e);
                e.printStackTrace();
            }

            //hasOptions checks if option is present or not
            if (cmd.hasOption("login")) {
                logger.info("Using cli argument -login=" + cmd.getOptionValue("login"));
                switch (cmd.getOptionValue("login")) {

                    case "nonPROD":
                        myS3API = new S3LoadClient(properties.getProperty("Host-nonPROD"),
                                properties.getProperty("Accesskey-nonPROD"),
                                properties.getProperty("Securitykey-nonPROD"));
                        break;

                    case "PROD":
                        myS3API = new S3LoadClient(properties.getProperty("Host-PROD"),
                                properties.getProperty("Accesskey-PROD"),
                                properties.getProperty("Securitykey-PROD"));
                        break;


                    default:
                        logger.error("Host unknown");
                        HelpFormatter formatter = new HelpFormatter();
                        formatter.printHelp("S3Client", options);
                        System.exit(0);
                        break;
                }

            }

            //Create new bucket
            if (cmd.hasOption("createBucket")) {
                logger.info("Using cli argument -createBucket=" + cmd.getOptionValue("createBucket"));

                //Check if the bucket should be created versioned
                boolean enabledVersioning = Boolean.parseBoolean(properties.getProperty("BucketVersioning"));

                assert myS3API != null;
                myS3API.createBucket(cmd.getOptionValue("createBucket"), enabledVersioning);
            }

            //Delete bucket with content
            if (cmd.hasOption("deleteBucket")) {
                logger.info("Using cli argument -deleteBucket=" + cmd.getOptionValue("deleteBucket"));
                assert myS3API != null;
                myS3API.deleteBucket(cmd.getOptionValue("deleteBucket"));
            }

            //Upload a new file to an existing bucket
            if (cmd.hasOption("uploadFile")) {
                logger.info("Using cli argument -uploadFile");
                logger.info("===== Upload Properties =====");
                logger.info("Upload Bucket: " + properties.getProperty("UploadBucketName"));
                logger.info("Upload Objectname: " + properties.getProperty("UploadObjectname"));
                logger.info("Upload Filename: " + properties.getProperty("UploadFilename"));

                assert myS3API != null;
                myS3API.putObject(properties.getProperty("UploadBucketName"), properties.getProperty("UploadObjectname"), properties.getProperty("UploadFilename"));
            }


            //Download a file
            if (cmd.hasOption("downloadFile")) {
                logger.info("Using cli argument -downloadFile");
                logger.info("===== Download Properties =====");
                logger.info("Download Bucket: " + properties.getProperty("DownloadFileBucketName"));
                logger.info("Download Objectname: " + properties.getProperty("DownloadObjectname"));
                logger.info("Download Version ID: " + properties.getProperty("DownloadVersionID"));

                assert myS3API != null;
                myS3API.getObjectVersion(properties.getProperty("DownloadFileBucketName"), properties.getProperty("DownloadObjectname"), properties.getProperty("DownloadVersionID"));
            }
            //Lists all objects of a bucket and exports them as file
            if (cmd.hasOption("listBucketToFile")) {
                logger.info("Using cli argument -listBucketToFile=" + cmd.getOptionValue("listBucketToFile"));
                logger.info("Exported File with all objects of the bucket is: " + "Export_" + cmd.getOptionValue("listBucketToFile") + ".csv");
                assert myS3API != null;
                myS3API.listObjects(cmd.getOptionValue("listBucketToFile"), "Export_" + cmd.getOptionValue("listBucketToFile") + ".csv");
            }
        } catch (ParseException e) {
            logger.error("Failed to parse command line properties");
            help();
        }
    }

    private static void help() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("S3Client", options);
        System.exit(0);
    }
}
