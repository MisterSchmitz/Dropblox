package surfstore;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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

    private final ArrayList<ManagedChannel> metadataChannels = new ArrayList<>();
    private final ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub> metadataStubs = new ArrayList<>();

    public Client(ConfigReader config) {
        this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(config.getLeaderNum()))
                .usePlaintext(true).build();
        this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.config = config;

        // Get all metadataservers: Mainly for testing
        for (int i = 1; i <= config.getNumMetadataServers(); i++) {
            int currPort = config.metadataPorts.get(i);
            ManagedChannel metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1",
                    config.getMetadataPort(i)).usePlaintext(true).build();
            this.metadataChannels.add(metadataChannel);
            this.metadataStubs.add(MetadataStoreGrpc.newBlockingStub(metadataChannel));
        }
    }

    public void shutdown() throws InterruptedException {
        metadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
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

    private void go(String operation, String filename, String pathToStoreDownload, Integer serverId) {
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
        if(operation.equals("getversion")) {
            getVersion(filename);
        }
        if(operation.equals("crash")) {
            crashServer(serverId);
        }
        if(operation.equals("restore")) {
            restoreServer(serverId);
        }

    }

    /*
     * Reads the local file, creates a set of hashed blocks and uploads them onto the MetadataStore
     * (and potentially the BlockStore if they were not already present there).
     */
    private void upload(String filename) {
        System.err.println("Uploading file " + filename);

        // Read the local file
        File fileToUpload = new File(filename);
        byte[] fileContents;
        try {
            fileContents = Files.readAllBytes(fileToUpload.toPath());
        } catch (Exception e) {
            System.err.println("File not found on disk. Exiting.");
            System.out.println("Not Found");
            return;
        }

        int numBytes = fileContents.length;
        int numFullBlocks = numBytes / BLOCKSIZE;
        int numBytesRem = numBytes % BLOCKSIZE;
        int numBlocks = numFullBlocks;
        if (numBytesRem > 0)
            numBlocks+=1;
//
//        System.err.println("numFullBlocks " + numFullBlocks);
//        System.err.println("numBytesRem " + numBytesRem);

        // Create a set of Blocks
        Block[] blockList = new Block[numBlocks];
        ArrayList<String> hashList = new ArrayList<>();
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
            Block b = bytesToBlock(a);
            blockList[i] = b;
            hashList.add(b.getHash());
        }

        // Check if file exists in Metadatastore, and get Version
        System.err.println("Checking if file exists in Metadatastore.");
        FileInfo.Builder readReqBuilder = FileInfo.newBuilder();
        FileInfo readRequest = readReqBuilder.setFilename(filename).build();
        FileInfo readResponse = metadataStub.readFile(readRequest);
        int fileVersion = readResponse.getVersion();

        // If file does not exist, upload file to BlockStore
        if (fileVersion == 0) {
            System.out.println("Not Found");
            // Upload all Blocks to BlockStore
            for (Block block : blockList) {
                Block.Builder storeBlockReqBuilder = Block.newBuilder();
                storeBlockReqBuilder.setHash(block.getHash());
                storeBlockReqBuilder.setData(block.getData());
                blockStub.storeBlock(storeBlockReqBuilder.build());
            }
        }
        else {
            System.out.println("OK");
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
            System.err.println("Attempting upload again.");

            int currentVersion = modifyResponse.getCurrentVersion();
            ProtocolStringList missingBlocks = modifyResponse.getMissingBlocksList();

            if (modifyResult.getNumber() == 1)  // OLD_VERSION
                modifyReqBuilder.setVersion(currentVersion+1);

            if (modifyResult.getNumber() == 2)  // MISSING_BLOCKS
            {
                // Upload the missing Blocks to BlockStore
                System.err.println("Uploading missing blocks...");
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

            if (modifyResult.getNumber() == 3) {  // NOT_LEADER
                System.err.println("Upload failed. Non-leader.");
                return;
            }

            // Send new modify request
            modifyReqBuilder.setFilename(filename);
            modifyReqBuilder.clearBlocklist();
            modifyReqBuilder.addAllBlocklist(hashList);
            modifyResponse = metadataStub.modifyFile(modifyReqBuilder.build());
            modifyResult = modifyResponse.getResult();
        }

        System.out.println("OK");
        System.err.println("Upload Success.");
    }


    private void download(String filename, String pathToStoreDownload) {
        System.err.println("Downloading file " + filename);
        FileInfo.Builder builder = FileInfo.newBuilder();
        builder.setFilename(filename);
        FileInfo response = metadataStub.readFile(builder.build());

        if (response.getBlocklistCount() <= 0) {
            System.out.println("Not Found");
            return;
        }

        // Deleted file
        if (response.getBlocklistList().contains("0")) {
            System.out.println("Not Found");
            return;
        }

        ProtocolStringList blockList = response.getBlocklistList();

        if (!pathToStoreDownload.endsWith("/")) {
            pathToStoreDownload = pathToStoreDownload.concat("/");
        }

        // Create a set of Blocks
        HashMap<String, ByteString> localFileHashMap = new HashMap<>();
        ArrayList<String> localFileHashList = new ArrayList<>();

        // Check for existing blocks in ALL FILES in download directory
        File folder = new File(pathToStoreDownload);
        File[] listOfFiles = folder.listFiles();

        for (File file : listOfFiles)
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
                for (i = 0; i < numFullBlocks; i++) {
                    byte[] a = Arrays.copyOfRange(fileContents, i * BLOCKSIZE, (i + 1) * BLOCKSIZE);
                    Block b = bytesToBlock(a);
                    localFileHashMap.put(b.getHash(), b.getData());
                    localFileHashList.add(b.getHash());
                }
                // Last Small Block
                byte[] a = Arrays.copyOfRange(fileContents, i * BLOCKSIZE, numBytes);
                if (a.length > 0) {
                    Block b = bytesToBlock(a);
                    localFileHashMap.put(b.getHash(), b.getData());
                    localFileHashList.add(b.getHash());
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
        System.out.println("OK");
    }


    private void delete(String filename) {
        System.err.println("Deleting file " + filename);

        // Check if file exists in Metadatastore, and get Version
        FileInfo.Builder readReqBuilder = FileInfo.newBuilder();
        FileInfo readRequest = readReqBuilder.setFilename(filename).build();
        FileInfo readResponse = metadataStub.readFile(readRequest);
        int fileVersion = readResponse.getVersion();

        if (fileVersion == 0)
            return;

        FileInfo.Builder deleteReqBuilder = FileInfo.newBuilder();
        deleteReqBuilder.setFilename(filename);
        deleteReqBuilder.setVersion(fileVersion+1);

        WriteResult deleteResponse = metadataStub.deleteFile(deleteReqBuilder.build());
        System.out.println(deleteResponse.getResult().getValueDescriptor());
        System.err.println("Server Response: " + deleteResponse.getResult().getValueDescriptor());
    }


    private void getVersion(String filename) {
        System.err.println("Getting versions of file " + filename);

        FileInfo.Builder readReqBuilder = FileInfo.newBuilder();
        FileInfo readRequest = readReqBuilder.setFilename(filename).build();

        // Centralized version
        FileInfo response = metadataStub.getVersion(readRequest);
        System.out.println(response.getVersion());

        // Distributed Version
//        for (MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub : metadataStubs) {
//            int fversion = metadataStub.getVersion(readRequest).getVersion();
//            System.out.print(fversion + " ");
//        }
    }

    private void crashServer(int id) {
        int serverId = id;
        System.err.println("Crashing server " + serverId);
        metadataStubs.get(serverId-1).crash(Empty.newBuilder().build());
    }

    private void restoreServer(int id) {
        int serverId = id;
        System.err.println("Restoring server " + serverId);
        metadataStubs.get(serverId-1).restore(Empty.newBuilder().build());
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
        parser.addArgument("serverId").type(Integer.class).nargs("?").setDefault(-1)
                .help("Server to crash or restore");

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
                    c_args.getString("path_to_store"),
                    c_args.getInt("serverId")
            );
        } finally {
            client.shutdown();
        }
    }
}
