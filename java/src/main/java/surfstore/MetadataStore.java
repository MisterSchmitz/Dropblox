package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;
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
import surfstore.SurfStoreBasic.SimpleAnswer;
import surfstore.SurfStoreBasic.WriteResult;

import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

public final class MetadataStore {
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

    protected Server server;
	protected ConfigReader config;
    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    public MetadataStore(ConfigReader config) {
    	this.config = config;
        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

	}

	private void start(int port, int numThreads) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl())
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
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
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
        if (c_args == null){
            throw new RuntimeException("Argument parsing failed");
        }
        
        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        if (c_args.getInt("number") > config.getNumMetadataServers()) {
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
        }

        final ManagedChannel blockChannel;
        final BlockStoreGrpc.BlockStoreBlockingStub blockStub;
        blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        final MetadataStore server = new MetadataStore(config);
        server.start(config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"));
        server.blockUntilShutdown();
    }

    class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {
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
         * that is not the leader
         * </pre>
         */
        @Override
        public void readFile(FileInfo request, StreamObserver<FileInfo> responseObserver) {
            logger.info("Reading file" + request.getFilename());

            FileInfo.Builder responseBuilder = FileInfo.newBuilder();

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
            if (hashlist.containsKey(filename)) {
                responseBuilder.addAllBlocklist(hashlist.get(filename));
            }

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
        public void modifyFile(FileInfo request, StreamObserver<WriteResult> responseObserver) {
            String filename = request.getFilename();
            int version = request.getVersion();
            ProtocolStringList blockList = request.getBlocklistList();
            logger.info("Writing file: " + filename + "Version: " + version);

            WriteResult.Builder responseBuilder = WriteResult.newBuilder();
            responseBuilder.setResultValue(0);

            // TODO: Proper way to handle file not found when uploading?
            if (!this.version.containsKey(filename)){
                System.err.println("Could not find file version.");
            }

            responseBuilder.setCurrentVersion(this.version.get(filename));

            if (version != this.version.get(filename)+1) {
                responseBuilder.setResultValue(1); // OLD_VERSION
            }
            else {
                // Get missing blocks
                for (String hash : blockList) {
                    Block.Builder builder = Block.newBuilder();
                    builder.setHash(hash);
                    SimpleAnswer blockExists = blockStub.hasBlock(builder.build());
                    if (!blockExists.getAnswer())
                        responseBuilder.addMissingBlocks(hash);
                }
                if (responseBuilder.getMissingBlocksCount() > 0) {
                    responseBuilder.setResultValue(2); // MISSING_BLOCKS
                }
                else {
                    // TODO TEST: If version is exactly one more than current version, update hashlist
                    this.hashlist.put(filename, blockList);
                    this.version.put(filename, version);
                }

                if (responseBuilder.getResultValue() == 0) {
                    responseBuilder.setCurrentVersion(this.version.get(filename));
                }
            }

            WriteResult response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
//
//        /**
//         * <pre>
//         * Delete a file.
//         * This has the same semantics as ModifyFile, except that both the
//         * client and server will not specify a blocklist or missing blocks.
//         * As in ModifyFile, this call should return an error if the server
//         * it is called on isn't the leader
//         * </pre>
//         */
//        @Override
//        public void deleteFile(surfstore.SurfStoreBasic.FileInfo request,
//                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
//            asyncUnimplementedUnaryCall(METHOD_DELETE_FILE, responseObserver);
//        }
//
//        /**
//         * <pre>
//         * Query whether the MetadataStore server is currently the leader.
//         * This call should work even when the server is in a "crashed" state
//         * </pre>
//         */
//        @Override
//        public void isLeader(surfstore.SurfStoreBasic.Empty request,
//                             io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
//            asyncUnimplementedUnaryCall(METHOD_IS_LEADER, responseObserver);
//        }
//
//        /**
//         * <pre>
//         * "Crash" the MetadataStore server.
//         * Until Restore() is called, the server should reply to all RPCs
//         * with an error (except Restore) and not send any RPCs to other servers.
//         * </pre>
//         */
//        @Override
//        public void crash(surfstore.SurfStoreBasic.Empty request,
//                          io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
//            asyncUnimplementedUnaryCall(METHOD_CRASH, responseObserver);
//        }
//
//        /**
//         * <pre>
//         * "Restore" the MetadataStore server, allowing it to start
//         * sending and responding to all RPCs once again.
//         * </pre>
//         */
//        @Override
//        public void restore(surfstore.SurfStoreBasic.Empty request,
//                            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
//            asyncUnimplementedUnaryCall(METHOD_RESTORE, responseObserver);
//        }
//
//        /**
//         * <pre>
//         * Find out if the node is crashed or not
//         * (should always work, even if the node is crashed)
//         * </pre>
//         */
//        @Override
//        public void isCrashed(surfstore.SurfStoreBasic.Empty request,
//                              io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
//            asyncUnimplementedUnaryCall(METHOD_IS_CRASHED, responseObserver);
//        }
//
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
        public void getVersion(FileInfo request, StreamObserver<FileInfo> responseObserver) {
//            asyncUnimplementedUnaryCall(METHOD_GET_VERSION, responseObserver);
        }

        private Map<String, Integer> version = new HashMap<String, Integer>();
        private Map<String, ProtocolStringList> hashlist = new HashMap<String, ProtocolStringList>();
    }
}