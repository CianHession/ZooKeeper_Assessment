import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ServiceRegistry implements Watcher {

    private static final String SERVICES_NAMESPACE = "/services";
    private final String zookeeperAddress;
    private final int sessionTimeout;
    private ZooKeeper zooKeeper;

    public ServiceRegistry(String zookeeperAddress, int sessionTimeout) {
        this.zookeeperAddress = zookeeperAddress;
        this.sessionTimeout = sessionTimeout;
    }

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(zookeeperAddress, sessionTimeout, this);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None:
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to Zookeeper");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected from Zookeeper event");
                        zooKeeper.notifyAll();
                    }
                }
                break;
                // To do: alert of changes to node, update, make services know of one another
            case NodeChildrenChanged:
                try {
                    getServices().values().forEach(System.out::println);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                }

                break;
        }
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    public void initialise() throws InterruptedException, KeeperException {
        // Create the services namespace if it doesn't already exist
        if (this.checkParentNodeExists()) {
            System.out.println("Parent node already exists, skipping creation");
            zooKeeper.getChildren(SERVICES_NAMESPACE, this);
            System.out.println(zooKeeper.getChildren(SERVICES_NAMESPACE, this));
        } else {
            //It does not exist, so we can create the node
            System.out.println("Creating parent node");
            createParentNode();
        }

    }

    private void createParentNode() {
        try {
            // Create parent node
            zooKeeper.create(SERVICES_NAMESPACE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("Parent node created");
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            System.out.println("Parent node creation failed");
        }
    }

    private boolean checkParentNodeExists() throws InterruptedException, KeeperException {
        // Check if parent node exists in Zookeeper
        return zooKeeper.exists(SERVICES_NAMESPACE, false) != null;
    }

    public Map<String, String> getServices() throws InterruptedException, KeeperException {
        Map<String, String> services = new HashMap<>();
        // Get all the children of the parent node if they exist
        if (zooKeeper.getChildren(SERVICES_NAMESPACE, this) != null) {
            List<String> serviceNames = zooKeeper.getChildren(SERVICES_NAMESPACE, this);
            // For each child, get the data and add it to the map
            for (String serviceName : serviceNames) {
                String servicePath = SERVICES_NAMESPACE + "/" + serviceName;
                String serviceAddress = new String(zooKeeper.getData(servicePath, this, new Stat()));
                services.put(serviceName, serviceAddress);
            }
        } else {
            System.out.println("No services found");
        }
        // Return the map of services
        return services;
    }


    public void register(String serviceName, String serviceAddress) throws InterruptedException, KeeperException {
        String servicePath = SERVICES_NAMESPACE + "/" + serviceName;
        // Check if the service already exists  - if it does, update the address
        if (zooKeeper.exists(servicePath, false) == null) {
            System.out.println("Creating service node: " + servicePath);
            String createdPath = zooKeeper.create(servicePath, serviceAddress.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println("Created path: " + createdPath);
        } else {
            System.out.println("Service node already exists: " + servicePath);
        }
    }
}
