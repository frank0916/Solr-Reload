import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class OrderManager {

    private static String[] authToken = null;

    private static final String CONSUMER_ID = "62a55923-8447-4205-b2f9-d712b35d8b27";

    private static final String AUTH_URL = "https://sr-validation.stage.wm.com/getAuth?env=prod";
    private static final String RELOAD_URL = "https://os-po-prim-solr-ndc-consumer-app.prod.wm.com/purchase-orders-consumer/services/v4/ingest/reload/order";

    private static HttpClient client;

    public static void main(String[] args) {
        try {
            initClient();
            startOrderReloadLoop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void initClient() throws Exception {
        TrustManager[] trustAllCerts = new TrustManager[]{
            new X509TrustManager() {
                public X509Certificate[] getAcceptedIssuers() { return null; }
                public void checkClientTrusted(X509Certificate[] certs, String authType) {}
                public void checkServerTrusted(X509Certificate[] certs, String authType) {}
            }
        };
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustAllCerts, new SecureRandom());
        client = HttpClient.newBuilder().sslContext(sslContext).build();
    }

    private static synchronized void auth() throws Exception {
        while (true) {
            HttpRequest authRequest = HttpRequest.newBuilder()
                    .uri(URI.create(AUTH_URL))
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(authRequest, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200 ) continue;

            String body = response.body();

            // Matches 'inTimestamp: ' followed by digits
            Matcher tMatcher = Pattern.compile("inTimestamp:\\s*(\\d+)").matcher(body);
            if (!tMatcher.find()) continue;

            // Matches 'authSignature: ' followed by all characters until the end of the line/string
            Matcher sMatcher = Pattern.compile("authSignature:\\s*(\\S+)").matcher(body);
            if (!sMatcher.find()) continue;

            String[] token = new String[2];
            token[0] = tMatcher.group(1);
            token[1] = sMatcher.group(1);
            authToken = token;
            return;
        }
    }

    private static void startOrderReloadLoop() throws Exception {
        auth();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        // Runs immediately, then repeats every 4 minutes
        scheduler.scheduleAtFixedRate(() -> {
            try {
                auth();
            } catch (Exception e) {
            }
        }, 0, 1, TimeUnit.MINUTES);

        ExecutorService executorService = Executors.newFixedThreadPool(250);
        Scanner scanner = new Scanner(System.in);

        while (scanner.hasNextLine()) {
            String poNo = scanner.nextLine().trim();
            if (poNo.isEmpty()) continue;

            executorService.submit(() -> {
                try {
                    String jsonPayload = String.format("{\"purchaseOrderNo\": \"%s\"}", poNo);
                    HttpRequest reloadRequest = HttpRequest.newBuilder()
                            .uri(URI.create(RELOAD_URL))
                            .header("Content-Type", "application/json")
                            .header("WM_SVC.VERSION", "4.0.0")
                            .header("WM_SVC.ENV", "prod406")
                            .header("WM_SVC.NAME", "PURCHASE-ORDER-SERVICE")
                            .header("WM_CONSUMER.ID", CONSUMER_ID)
                            .header("wm_consumer.intimestamp", authToken[0])
                            .header("wm_sec.auth_signature", authToken[1])
                            .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                            .build();

                    HttpResponse<Void> response = client.send(reloadRequest, HttpResponse.BodyHandlers.discarding());
                    System.out.println("PO: " + poNo + " Status: " + response.statusCode());

                } catch (Exception e) {
                    System.err.println("Error on PO " + poNo + ": " + e.getMessage());
                }
            });
        }
        scanner.close();
        scheduler.shutdown();
        executorService.shutdown();
    }
}
