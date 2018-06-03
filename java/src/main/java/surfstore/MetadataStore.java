package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import com.google.protobuf.ProtocolStringList;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.MetaLog;
import surfstore.SurfStoreBasic.SimpleAnswer;
import surfstore.SurfStoreBasic.WriteResult;

import static surfstore.MetadataStoreGrpc.*;

public final class MetadataStore {
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

    private Server server;
    private ConfigReader config;
    private boolean isLeader;
    private boolean isCrashed;

    public MetadataStore(ConfigReader config) {
        this.config = config;
        this.isCrashed = false;
    }

    private void start(int port, int numThreads) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl(this.config, port))
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();

        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MetadataStore.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) server.shutdown();
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) server.awaitTermination();
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("MetadataStore").build()
                .description("MetadataStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("-n", "--number").type(Integer.class).setDefault(1)
                .help("Set which number this server is");
        parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
                .help("Maximum number of concurrent threads");

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
        if (c_args == null) throw new RuntimeException("Argument parsing failed");

        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        if (c_args.getInt("number") > config.getNumMetadataServers())
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));

        final MetadataStore server = new MetadataStore(config);
        server.start(
                config.getMetadataPort(c_args.getInt("number")),
                c_args.getInt("threads"));
        server.blockUntilShutdown();
    }

    class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {

        protected ConfigReader config;

        private final ManagedChannel blockChannel;
        private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

        private final ArrayList<ManagedChannel> metadataChannels = new ArrayList<>();
        private final ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub> metadataStubs = new ArrayList<>();

        public MetadataStoreImpl(ConfigReader config, int port) {
            this.config = config;
            this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                    .usePlaintext(true).build();
            this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

            // Figure out which server this is, and whether it is leader
            for (int i=1; i<=config.getNumMetadataServers(); i++) {
                int currPort = config.metadataPorts.get(i);
                if (currPort == port) {
                    System.err.println("serverId: " + i);
                    if (config.getLeaderNum() == i) {
                        isLeader = true;
                        System.err.println("This server is leader.");
                    } else isLeader = false;
                    break;
                }
            }

            // If leader, add followers
            if(isLeader) {
                int followerCount = 0;
                for (int i = 1; i <= config.getNumMetadataServers(); i++) {
                    int currPort = config.metadataPorts.get(i);
                    if (currPort == port) continue;
                    ManagedChannel metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1",
                            config.getMetadataPort(i)).usePlaintext(true).build();
                    this.metadataChannels.add(metadataChannel);
                    this.metadataStubs.add(MetadataStoreGrpc.newBlockingStub(metadataChannel));
                    followerCount += 1;
                }
                System.err.println("Added "+followerCount+" servers as followers");
            }
            else { // Add leader
                System.err.println("Adding server " + config.getLeaderNum() + " as leader");
                ManagedChannel metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1",
                        config.getMetadataPort(config.getLeaderNum())).usePlaintext(true).build();
                this.metadataChannels.add(metadataChannel);
                this.metadataStubs.add(MetadataStoreGrpc.newBlockingStub(metadataChannel));
            }

        }

        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        /**
         * <pre>
         * Read the requested file.
         * The client only needs to supply the "filename" argument of FileInfo.
         * The server only needs to fill the "version" and "blocklist" fields.
         * If the file does not exist, "version" should be set to 0.
         *
         * This command should return an error if it is called on a server
         * that is not the leader FROM PIAZZA:
         * Set the filename and the version to the current version of the file as stored on that follower.
         * </pre>
         */
        @Override
        public void readFile(surfstore.SurfStoreBasic.FileInfo request,
                             io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {
            FileInfo.Builder responseBuilder = FileInfo.newBuilder();

            logger.info("Reading file " + request.getFilename());

            // Filename
            String filename = request.getFilename();
            responseBuilder.setFilename(filename);

            // Version
            int v;
            if (!version.containsKey(filename))
                this.version.put(filename, 0);

            v = version.get(filename);
            responseBuilder.setVersion(v);

            // Blocklist
            if (hashlist.containsKey(filename)) responseBuilder.addAllBlocklist(hashlist.get(filename));

            FileInfo response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        }

        /**
         * <pre>
         * Write a file.
         * The client must specify all fields of the FileInfo message.
         * The server returns the result of the operation in the "result" field.
         *
         * The server ALWAYS sets "current_version", regardless of whether
         * the command was successful. If the write succeeded, it will be the
         * version number provided by the client. Otherwise, it is set to the
         * version number in the MetadataStore.
         *
         * If the result is MISSING_BLOCKS, "missing_blocks" contains a
         * list of blocks that are not present in the BlockStore.
         *
         * This command should return an error if it is called on a server
         * that is not the leader
         * </pre>
         */
        @Override
        public void modifyFile(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
            WriteResult.Builder responseBuilder = WriteResult.newBuilder();

            if (!isLeader) {
                System.err.println("Server is not Leader");
                responseBuilder.setResultValue(3); //NOT_LEADER
            }
            else if (isCrashed) {
                System.err.println("Server is crashed");
                responseBuilder.setResultValue(3); //NOT_LEADER
            }
            else {
                String filename = request.getFilename();
                int version = request.getVersion();
                ProtocolStringList blockList = request.getBlocklistList();
                ArrayList<String> newBlockList = new ArrayList<>();

                System.err.println("Attempting to write file" + filename + "Version: " + version);

                if (!this.version.containsKey(filename)) this.version.put(filename, 0);

                responseBuilder.setCurrentVersion(this.version.get(filename));

                if (version != this.version.get(filename) + 1) responseBuilder.setResultValue(1); // OLD_VERSION
                else {
                    // Get missing blocks
                    for (String hash : blockList) {
                        newBlockList.add(hash);
                        Block.Builder builder = Block.newBuilder();
                        builder.setHash(hash);
                        SimpleAnswer blockExists = blockStub.hasBlock(builder.build());
                        if (!blockExists.getAnswer())
                            responseBuilder.addMissingBlocks(hash);
                    }
                    if (responseBuilder.getMissingBlocksCount() > 0)
                        responseBuilder.setResultValue(2); // MISSING_BLOCKS
                    else {
                        // TODO: Make this entire operation atomic
                        // Prepare log entry
                        FileInfo.Builder logAppendBuilder = FileInfo.newBuilder();
                        logAppendBuilder.setFilename(filename);
                        logAppendBuilder.setVersion(version);
                        logAppendBuilder.addAllBlocklist(newBlockList);
                        FileInfo logAppendRequest = logAppendBuilder.build();

                        // Get 'consensus' from followers, by sending log update
                        boolean consensusReached = false;
                        if (metadataStubs.size() > 0) {
                            for (MetadataStoreBlockingStub metadatastub : metadataStubs) {
                                SimpleAnswer response = metadatastub.log(logAppendRequest);
                                System.err.println("Log received by follower: " + response.getAnswer());
                                if (response.getAnswer())
                                    consensusReached = true;
                            }
                        } else {
                            consensusReached = true;
                        }

                        if (consensusReached) {
                            // Commit transaction in own state
                            this.version.put(filename, version);
                            this.hashlist.put(filename, newBlockList);
                            logger.info("Writing file " + filename + "Version: " + version);

                            // Send commit message to followers
                            for (MetadataStoreBlockingStub metadatastub : metadataStubs) {
                                SimpleAnswer commitResponse = metadatastub.commit(logAppendRequest);
                                System.err.println("Commit message received by follower: " + commitResponse.getAnswer());
                            }

                            responseBuilder.setResultValue(0);
                        }
                    }

                    if (responseBuilder.getResultValue() == 0)
                        responseBuilder.setCurrentVersion(this.version.get(filename));
                }
            }

            WriteResult response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        /**
         * <pre>
         * Delete a file.
         * This has the same semantics as ModifyFile, except that both the
         * client and server will not specify a blocklist or missing blocks.
         * As in ModifyFile, this call should return an error if the server
         * it is called on isn't the leader
         * </pre>
         */
        @Override
        public void deleteFile(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
            WriteResult.Builder responseBuilder = WriteResult.newBuilder();

            if (!isLeader) {
                System.err.println("Server is not Leader");
                responseBuilder.setResultValue(3); //NOT_LEADER
            }
            else if (isCrashed) {
                System.err.println("Server is crashed");
                responseBuilder.setResultValue(3); //NOT_LEADER
            }
            else {
                String filename = request.getFilename();
                int version = request.getVersion();
                logger.info("Attempting to Delete file " + filename + "Version: " + version);

                if(!this.version.containsKey(filename)) {
                    responseBuilder.setResultValue(0);
                    responseBuilder.setCurrentVersion(0);
                }
                else if (version != this.version.get(filename) + 1) {
                    responseBuilder.setResultValue(1); // OLD_VERSION
                    responseBuilder.setCurrentVersion(this.version.get(filename));
                } else {
                    ArrayList<String> newBlockList = new ArrayList<>();
                    newBlockList.add("0");
                    this.hashlist.put(filename, newBlockList);

                    // Prepare log entry
                    FileInfo.Builder logAppendBuilder = FileInfo.newBuilder();
                    logAppendBuilder.setFilename(filename);
                    logAppendBuilder.setVersion(version);
                    logAppendBuilder.addAllBlocklist(newBlockList);
                    FileInfo logAppendRequest = logAppendBuilder.build();

                    // Get 'consensus' from followers, by sending log update
                    boolean consensusReached = false;
                    if (metadataStubs.size() > 0) {
                        for (MetadataStoreBlockingStub metadatastub : metadataStubs) {
                            SimpleAnswer response = metadatastub.log(logAppendRequest);
                            System.err.println("Log received by follower: " + response.getAnswer());
                            if (response.getAnswer())
                                consensusReached = true;
                        }
                    } else {
                        consensusReached = true;
                    }

                    if (consensusReached) {
                        // Commit transaction in own state
                        this.version.put(filename, version);
                        this.hashlist.put(filename, newBlockList);
                        logger.info("Deleting file " + filename + "Version: " + version);

                        // Send commit message to followers
                        for (MetadataStoreBlockingStub metadatastub : metadataStubs) {
                            SimpleAnswer commitResponse = metadatastub.commit(logAppendRequest);
                            System.err.println("Commit message received by follower: " + commitResponse.getAnswer());
                        }

                        // Prepare client response
                        responseBuilder.setResultValue(0);
                        responseBuilder.setCurrentVersion(this.version.get(filename) + 1);
                    }
                }
            }

            WriteResult response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void log(surfstore.SurfStoreBasic.FileInfo request,
                        io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
            String fname = request.getFilename();
            int fversion = request.getVersion();
//            ProtocolStringList fblocklist = request.getBlocklistList();
//            ArrayList<String> newBlockList = new ArrayList<>(fblocklist);

            if (!isCrashed) logger.info("Logged version " + fversion + " changes to file " + fname);

            SimpleAnswer.Builder responseBuilder = SimpleAnswer.newBuilder().setAnswer(!isCrashed);
            SimpleAnswer response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void commit(surfstore.SurfStoreBasic.FileInfo request,
                           io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
            String fname = request.getFilename();
            int fversion = request.getVersion();
            ProtocolStringList fblocklist = request.getBlocklistList();
            ArrayList<String> newBlockList = new ArrayList<>(fblocklist);

            if (!isCrashed) {
                this.version.put(fname, fversion);
                this.hashlist.put(fname, newBlockList);
                logger.info("Committed version " + fversion + " changes to file " + fname);
            }

            SimpleAnswer.Builder responseBuilder = SimpleAnswer.newBuilder().setAnswer(!isCrashed);
            SimpleAnswer response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        /**
         * <pre>
         * Query whether the MetadataStore server is currently the leader.
         * This call should work even when the server is in a "crashed" state
         * </pre>
         */
        @Override
        public void isLeader(surfstore.SurfStoreBasic.Empty request,
                             io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {

            SimpleAnswer.Builder responseBuilder = SimpleAnswer.newBuilder();
            responseBuilder.setAnswer(isLeader);

            SimpleAnswer response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        /**
         * <pre>
         * "Crash" the MetadataStore server.
         * Until Restore() is called, the server should reply to all RPCs
         * with an error (except Restore) and not send any RPCs to other servers.
         * </pre>
         */
        @Override
        public void crash(surfstore.SurfStoreBasic.Empty request,
                          io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
            isCrashed = true;

            Empty.Builder responseBuilder = Empty.newBuilder();
            Empty response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

            logger.info("Server crashed");
        }

        /**
         * <pre>
         * "Restore" the MetadataStore server, allowing it to start
         * sending and responding to all RPCs once again.
         * </pre>
         */
        @Override
        public void restore(surfstore.SurfStoreBasic.Empty request,
                            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
            isCrashed = false;

            // Get log from leader
            MetadataStoreBlockingStub leader = metadataStubs.get(0);
            List<FileInfo> leaderLog = leader.getUpToSpeed(Empty.newBuilder().build()).getMetalogList();

            // Update own log
            for (FileInfo entry : leaderLog) {
                String fname = entry.getFilename();
                int fversion = entry.getVersion();
                ProtocolStringList fblocklist = entry.getBlocklistList();
                ArrayList<String> newBlockList = new ArrayList<>(fblocklist);
                this.version.put(fname, fversion);
                this.hashlist.put(fname, newBlockList);
            }

            Empty.Builder responseBuilder = Empty.newBuilder();
            Empty response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

            logger.info("Server restored");
        }

        /**
         * <pre>
         * Find out if the node is crashed or not
         * (should always work, even if the node is crashed)
         * </pre>
         */
        @Override
        public void isCrashed(surfstore.SurfStoreBasic.Empty request,
                              io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {

            SimpleAnswer.Builder responseBuilder = SimpleAnswer.newBuilder();
            responseBuilder.setAnswer(isCrashed);

            SimpleAnswer response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        /**
         * <pre>
         * Returns the current committed version of the requested file
         * The argument's FileInfo only has the "filename" field defined
         * The FileInfo returns the filename and version fields only
         * This should return a result even if the follower is in a
         *   crashed state
         * </pre>
         */
        @Override
        public void getVersion(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {
            String filename = request.getFilename();
            logger.info("Getting version of file: " + filename);

            FileInfo.Builder responseBuilder = FileInfo.newBuilder();

            responseBuilder.setFilename(filename);

            int v;
            if (!version.containsKey(filename)) {
                v=0;
                logger.info("File: " + filename + " Not found.");
            }
            else {
                v = version.get(filename);
                logger.info("File: " + filename + " Version: " + v);
            }
            responseBuilder.setVersion(v);

            FileInfo response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void getUpToSpeed(surfstore.SurfStoreBasic.Empty request,
                                 io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.MetaLog> responseObserver) {
            MetaLog.Builder responseBuilder = MetaLog.newBuilder();

            for (String filename : this.version.keySet()) {
                FileInfo.Builder fileInfoBuilder = FileInfo.newBuilder();
                fileInfoBuilder.setFilename(filename);
                fileInfoBuilder.setVersion(this.version.get(filename));
                fileInfoBuilder.addAllBlocklist(this.hashlist.get(filename));
                responseBuilder.addMetalog(fileInfoBuilder.build());
            }

            MetaLog response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        private Map<String, Integer> version = new HashMap<>();
        private Map<String, ArrayList<String>> hashlist = new HashMap<>();
    }
}