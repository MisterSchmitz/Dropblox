package surfstore;

import java.nio.charset.StandardCharsets;
        import java.security.MessageDigest;
        import java.security.NoSuchAlgorithmException;
        import java.util.Base64;

public class HashUtils {

    public static String sha256(String s) {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.exit(2);
        }

        byte[] hash = digest.digest(s.getBytes(StandardCharsets.UTF_8));
        String encoded = Base64.getEncoder().encodeToString(hash);

        return encoded;
    }

    // TODO: Come back if needed for download
//    public static byte[] decodeSha256(String s) {
//        MessageDigest digest = null;
//        try {
//            digest = MessageDigest.getInstance("SHA-256");
//        } catch (NoSuchAlgorithmException e) {
//            e.printStackTrace();
//            System.exit(2);
//        }
//
//        return Base64.getDecoder().decode(s);
//    }

}