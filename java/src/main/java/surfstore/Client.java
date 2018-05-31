package surfstore;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
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
import surfstore.SurfStoreBasic.WriteResult;

public final class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel metadataChannel;
    private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;

    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    private final ConfigReader config;

    private final int BLOCKSIZE = 4096;

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

    private static Block bytesToBlock(byte[] b) {
        Builder builder = Block.newBuilder();

        try {
            builder.setData(ByteString.copyFrom(b));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        builder.setHash(HashUtils.sha256(b));

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
        byte[] fileContents;
        String fileContentsString;
        try {
            fileContents = Files.readAllBytes(fileToUpload.toPath());
            fileContentsString = new String(fileContents, "UTF-8");
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        System.out.println("Read local file.");
//        System.out.println(fileContentsString);

        int numBytes = fileContents.length;

        int numFullBlocks = numBytes / BLOCKSIZE;
        int numBytesRem = numBytes % BLOCKSIZE;
        int numBlocks = numFullBlocks+1;

        System.out.println("numBlocks " + numFullBlocks);
        System.out.println("numBytesRem " + numBytesRem);

        // Create a set of hashed blocks
        Block[] blockList = new Block[numBlocks];
//        String[] hashList = new String[numBlocks];
        ArrayList<String> hashList = new ArrayList<String>();
        int i;
        for (i=0; i<numFullBlocks; i++) {
            byte[] a = Arrays.copyOfRange(fileContents, i*BLOCKSIZE, (i+1)*BLOCKSIZE);
            Block b = bytesToBlock(a);
            blockList[i] = b;
//            hashList[i] = b.getHash();
            hashList.add(b.getHash());
        }
        byte[] a = Arrays.copyOfRange(fileContents, i*BLOCKSIZE, numBytes);
        Block b = bytesToBlock(a);
        blockList[i] = b;
//        hashList[i] = b.getHash();
        hashList.add(b.getHash());

        // Read from Metadatastore
        FileInfo.Builder readReqBuilder = FileInfo.newBuilder();
        FileInfo readRequest = readReqBuilder.setFilename(filename).build();
        FileInfo readResponse = metadataStub.readFile(readRequest);
        int fileVersion = readResponse.getVersion();
        System.out.println("Version: "+ fileVersion);
        System.out.println("BlocklistCount: "+ readResponse.getBlocklistCount());

        // Upload to Metadatastore
        FileInfo.Builder modifyReqBuilder = FileInfo.newBuilder();
        FileInfo modifyRequest;
        WriteResult modifyResponse;

        // If not found, upload file
        if (fileVersion == 0) {
            modifyReqBuilder.setFilename(filename);
            modifyReqBuilder.setVersion(fileVersion + 1);
//            for (i=0; i<numBlocks; i++) {
//                modifyReqBuilder.setBlocklist(i, blockList[i].getHash());
//            }
//            modifyReqBuilder.addAllBlocklist(hashList);
            modifyReqBuilder.addAllBlocklist(hashList);
            modifyRequest = modifyReqBuilder.build();
            modifyResponse = metadataStub.modifyFile(modifyRequest);
        // TODO: Else, upload only the changed parts

            WriteResult.Result modifyResult = modifyResponse.getResult();
            int currentVersion = modifyResponse.getCurrentVersion();
            System.out.println("modifyResult: "+modifyResult.getValueDescriptor());
            System.out.println("currentVersion: "+currentVersion);
        }
    }

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
