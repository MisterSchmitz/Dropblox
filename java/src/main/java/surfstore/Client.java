package surfstore;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;

import com.google.protobuf.ProtocolStringList;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.Block;
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
        Block.Builder builder = Block.newBuilder();

        try {
            builder.setData(ByteString.copyFrom(s, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        builder.setHash(HashUtils.sha256(s));

        return builder.build(); // turns the Builder into a Block
    }

    private static Block bytesToBlock(byte[] b) {
        Block.Builder builder = Block.newBuilder();

        try {
            builder.setData(ByteString.copyFrom(b));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        builder.setHash(HashUtils.sha256(b));

        return builder.build(); // turns the Builder into a Block
    }

    private void go(String operation, String filename, String pathToStoreDownload) {
        metadataStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Metadata server");

        blockStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Blockstore server");


        if(operation.equals("upload")) {
            upload(filename);
        }
        if(operation.equals("download")) {
            download(filename, pathToStoreDownload);
        }
        if(operation.equals("delete")) {
            delete(filename);
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
        System.err.println("Uploading file " + filename);

        byte[] fileContents = readLocalFile(filename);
        int numBytes = fileContents.length;
        int numFullBlocks = numBytes / BLOCKSIZE;
        int numBytesRem = numBytes % BLOCKSIZE;
        int numBlocks = numFullBlocks;
        if (numBytesRem > 0)
            numBlocks+=1;

        System.err.println("numFullBlocks " + numFullBlocks);
        System.err.println("numBytesRem " + numBytesRem);

        // Create a set of Blocks
        Block[] blockList = new Block[numBlocks];
        ArrayList<String> hashList = new ArrayList<String>();
        // First 4KB Blocks
        int i;
        for (i=0; i<numFullBlocks; i++) {
            byte[] a = Arrays.copyOfRange(fileContents, i*BLOCKSIZE, (i+1)*BLOCKSIZE);
            Block b = bytesToBlock(a);
            blockList[i] = b;
            hashList.add(b.getHash());
        }
        // Last Small Block
        byte[] a = Arrays.copyOfRange(fileContents, i*BLOCKSIZE, numBytes);
        if (a.length > 0) {
//            System.out.println("creating small block");
            Block b = bytesToBlock(a);
            blockList[i] = b;
            hashList.add(b.getHash());
        }

        // Check if file exists in Metadatastore, and get Version
        FileInfo.Builder readReqBuilder = FileInfo.newBuilder();
        FileInfo readRequest = readReqBuilder.setFilename(filename).build();
        FileInfo readResponse = metadataStub.readFile(readRequest);
        int fileVersion = readResponse.getVersion();
//        System.err.println("Server version: "+ fileVersion);

        // If file does not exist, upload file to BlockStore
        if (fileVersion == 0) {
            System.out.println("File does not exist. Creating...");
            // Upload all Blocks to BlockStore
            for (Block block : blockList) {
                Block.Builder storeBlockReqBuilder = Block.newBuilder();
                storeBlockReqBuilder.setHash(block.getHash());
                storeBlockReqBuilder.setData(block.getData());
                blockStub.storeBlock(storeBlockReqBuilder.build());
            }
        }

        // Send upload request to Metadatastore
        FileInfo.Builder modifyReqBuilder = FileInfo.newBuilder();
        WriteResult modifyResponse;

        modifyReqBuilder.setFilename(filename);
        modifyReqBuilder.setVersion(fileVersion+1);
        modifyReqBuilder.addAllBlocklist(hashList);
        modifyResponse = metadataStub.modifyFile(modifyReqBuilder.build());
        WriteResult.Result modifyResult = modifyResponse.getResult();

        System.err.println("First upload attempt: "+modifyResult.getValueDescriptor());
        // Do until receive OK response from MetadataStore
        while (modifyResult.getNumber() != 0) {
            System.out.println("Attempting upload again.");

            // TODO: BUG: Version is incorrectly updating even if file doesn't change

            int currentVersion = modifyResponse.getCurrentVersion();
            ProtocolStringList missingBlocks = modifyResponse.getMissingBlocksList();
            int missingBlockCount = modifyResponse.getMissingBlocksCount();

            // TODO: If version number is too old, update version and try again
            if (modifyResult.getNumber() == 1)  // OLD_VERSION
                modifyReqBuilder.setVersion(currentVersion+1);

            if (modifyResult.getNumber() == 2)  // MISSING_BLOCKS
            {
                // Upload the missing Blocks to BlockStore
                System.out.println("Uploading missing blocks...");
                // TODO: Somehow make this atomic across clients
                for (Block block : blockList) {
                    if (missingBlocks.contains(block.getHash())) {
                        Block.Builder storeBlockReqBuilder = Block.newBuilder();
                        storeBlockReqBuilder.setHash(block.getHash());
                        storeBlockReqBuilder.setData(block.getData());
                        blockStub.storeBlock(storeBlockReqBuilder.build());
                    }
                }
                modifyReqBuilder.setVersion(currentVersion+1);
            }

            if (modifyResult.getNumber() == 3)  // NOT_LEADER TODO: WHAT HAPPENS?
                break;

            // Send new modify request
            modifyReqBuilder.setFilename(filename);
            modifyReqBuilder.clearBlocklist();
            modifyReqBuilder.addAllBlocklist(hashList);
            modifyResponse = metadataStub.modifyFile(modifyReqBuilder.build());
            modifyResult = modifyResponse.getResult();

//            System.out.println("modifyResult: " + modifyResult.getValueDescriptor());
//            System.out.println("currentVersion: " + currentVersion);
//            System.out.println("missingBlockCount: " + missingBlockCount);
        }

        System.out.println("Upload Success.");
    }

    private byte[] readLocalFile(String filename) {
        // Read the local file
        File fileToUpload = new File(filename);
        byte[] fileContents;
        try {
            fileContents = Files.readAllBytes(fileToUpload.toPath());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return fileContents;
    }


    private void download(String filename, String pathToStoreDownload) {
        System.err.println("Downloading file " + filename);
        FileInfo.Builder builder = FileInfo.newBuilder();
        builder.setFilename(filename);
        FileInfo response = metadataStub.readFile(builder.build());

        if (response.getBlocklistCount() <= 0) {
            System.err.println("File not found. Aborting.");
            return;
        }

        ProtocolStringList blockList = response.getBlocklistList();

        if (!pathToStoreDownload.endsWith("/")) {
            pathToStoreDownload = pathToStoreDownload.concat("/");
        }

        // Create a set of Blocks
        HashMap<String, ByteString> localFileHashMap = new HashMap<String, ByteString>();
        ArrayList<String> localFileHashList = new ArrayList<String>();

        // Check for existing blocks in ALL FILES in download directory
        File folder = new File(pathToStoreDownload);
        File[] listOfFiles = folder.listFiles();

        for (File file : listOfFiles) {
            if (file.isFile()) {
                byte[] fileContents;
                try {
                    fileContents = Files.readAllBytes(file.toPath());
                } catch (Exception e) {
                    fileContents = new byte[0];
                }

                int numBytes = fileContents.length;
                int numFullBlocks = numBytes / BLOCKSIZE;

                // First 4KB Blocks
                int i;
                for (i=0; i<numFullBlocks; i++) {
                    byte[] a = Arrays.copyOfRange(fileContents, i*BLOCKSIZE, (i+1)*BLOCKSIZE);
                    Block b = bytesToBlock(a);
                    localFileHashMap.put(b.getHash(), b.getData());
                    localFileHashList.add(b.getHash());
                }
                // Last Small Block
                byte[] a = Arrays.copyOfRange(fileContents, i*BLOCKSIZE, numBytes);
                if (a.length > 0) {
                    Block b = bytesToBlock(a);
                    localFileHashMap.put(b.getHash(), b.getData());
                    localFileHashList.add(b.getHash());
                }
            }
        }

        byte[] allData = new byte[0];
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            outputStream.write(allData);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        // Get Blocks
        int downloadedBlockCounter = 0;
        for (String hash : blockList) {

            ByteString data;
            if (localFileHashList.contains(hash)) {
                data = localFileHashMap.get(hash);
            }
            else {
                Block.Builder blockBuilder = Block.newBuilder();
                blockBuilder.setHash(hash);
                Block getBlockResponse = blockStub.getBlock(blockBuilder.build());
                data = getBlockResponse.getData();
                downloadedBlockCounter += 1;
            }

            try {
                outputStream.write(data.toByteArray());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.err.println("Downloaded " + downloadedBlockCounter + " new blocks.");

        // Write to file
        byte outfileContents[] = outputStream.toByteArray();
        try {
            Files.write(Paths.get(pathToStoreDownload+filename), outfileContents);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        System.err.println(blockList.size() + " blocks written to file.");
    }

    // TODO: Implement delete
    private void delete(String filename) {
        System.err.println("Deleting file " + filename);
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
        parser.addArgument("path_to_store").type(String.class).nargs("?").setDefault("./")
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
