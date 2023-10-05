import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public class Application {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        if (args.length != 2) {
            System.out.println("USAGE: service-registry.jar serviceName serviceAddress");
            System.out.println("(The name and address that should be used to register this service should be provided as command-line arguments)");
            System.exit(0);
        }

        String serviceName = args[0];
        String serviceAddress = args[1];

        System.out.println("Creating Service Registry");
        ServiceRegistry registry = new ServiceRegistry(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT);
        registry.connectToZookeeper();

        System.out.println("Initialising Service Registry");
        registry.initialise();

        System.out.printf("Registering Service: %s, %s\n", serviceName, serviceAddress);
        registry.register(serviceName, serviceAddress);

        registry.run();
        registry.close();
    }
}
