package surfstore;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.Block.Builder;
import surfstore.SurfStoreBasic.FileInfo;

public final class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel metadataChannel;
    private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;

    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    private final ConfigReader config;

    public Client(ConfigReader config) {
        this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(1))
                .usePlaintext(true).build();
        this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.config = config;
    }

    public void shutdown() throws InterruptedException {
        metadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    private void ensure(boolean b) {
        if (b == false) {
            throw new RuntimeException("Assertion failed!");
        }
    }

    private static Block stringToBlock(String s) {
        Builder builder = Block.newBuilder();

        try {
            builder.setData(ByteString.copyFrom(s, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        builder.setHash(HashUtils.sha256(s));

        return builder.build(); // turns the Builder into a Block
    }

	private void go(String operation, String filename, String pathToStore) {
		metadataStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Metadata server");
        
//        blockStub.ping(Empty.newBuilder().build());
//        logger.info("Successfully pinged the Blockstore server");
//
//        // TODO: Implement your client here

        if(operation.equals("upload")) {
            upload(filename);
        }

//        Block b1 = stringToBlock("block_01");
//        Block b2 = stringToBlock("block_02");
//
//        ensure(blockStub.hasBlock(b1).getAnswer() == false);
//        ensure(blockStub.hasBlock(b2).getAnswer() == false);
//
//        blockStub.storeBlock(b1);
//        ensure(blockStub.hasBlock(b1).getAnswer() == true);
//
//        blockStub.storeBlock(b2);
//        ensure(blockStub.hasBlock(b2).getAnswer() == true);
//
//        Block b1prime = blockStub.getBlock(b1);
//        ensure(b1prime.getHash().equals(b1.getHash()));
//        ensure(b1prime.getData().equals(b1.getData()));
//
//        logger.info("We passed all the tests... yay!");
	}

	/*
     * Reads the local file, creates a set of hashed blocks and uploads them onto the MetadataStore
     * (and potentially the BlockStore if they were not already present there).
	 */
	private void upload(String filename) {
        System.out.println("Uploading file " + filename);

        // Read the local file
        File fileToUpload = new File(filename);
        String fileContents;
        try {
            fileContents = new String(Files.readAllBytes(fileToUpload.toPath()), "UTF-8");
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        System.out.println("Read local file.");
//        System.out.println(fileContents);

        // Create a set of hashed blocks
//        String fileContentsHash = HashUtils.sha256(fileContents);
//        System.out.println("Hashed local file.");
//        System.out.println(fileContentsHash);

        // Upload to Metadatastore
        FileInfo request = FileInfo.newBuilder().setFilename(filename).build();
        FileInfo response = metadataStub.readFile(request);
        System.out.println("version: "+ response.getVersion());
        System.out.println("hashlist: "+ response.getBlocklistCount());
    }

	/*
	 * TODO: Add command line handling here
	 */
    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("Client").build()
                .description("Client for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        // NEW
        parser.addArgument("operation").type(String.class)
                .help("Type of operation to perform");
        parser.addArgument("filename").type(String.class)
                .help("File name on which to operate");
        parser.addArgument("path_to_store").type(String.class).nargs("?").setDefault("")
                .help("Optional path to store downloads");
//        parser.addArgument("operation").type(String.class)
//                .help("Operation to perform");
//        parser.addArgument("filename1").type(String.class)
//                .help("file1");
//        parser.addArgument("filename2").type(String.class)
//                .help("file2");

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e){
            parser.handleError(e);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        Namespace c_args = parseArgs(args);
        if (c_args == null){
            throw new RuntimeException("Argument parsing failed");
        }

        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        Client client = new Client(config);
        
        try {
        	client.go(
        	        c_args.getString("operation"),
                    c_args.getString("filename"),
                    c_args.getString("path_to_store")
            );
        } finally {
            client.shutdown();
        }
    }

}
