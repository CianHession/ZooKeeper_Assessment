import com.github.blindpirate.extensions.CaptureSystemOutput;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.junit.jupiter.api.Assertions.*;

class ServiceRegistryTest {

    public static final int SLEEP_INTERVAL = 500;
    private static TestingServer zkServer;
    private static ZooKeeper zooKeeper;
    private static final String ZOOKEEPER_HOST = "localhost";
    private static final int ZOOKEEPER_PORT = 2182;
    private static final String ZOOKEEPER_ADDRESS = ZOOKEEPER_HOST + ":" + ZOOKEEPER_PORT;
    private static final int SESSION_TIMEOUT = 3000;
    private static final String SERVICES_PARENT_ZNODE = "/services";

    @BeforeAll
    public static void setUp() throws Exception {
        zkServer = new TestingServer(ZOOKEEPER_PORT, true);
        zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, watchedEvent -> {
        });
    }

    @AfterEach
    private void tearDown() throws KeeperException, InterruptedException {
        if (zooKeeper.exists(SERVICES_PARENT_ZNODE, false) != null) {
            ZKUtil.deleteRecursive(zooKeeper, SERVICES_PARENT_ZNODE);
        }
    }

    @AfterAll
    public static void shutDown() throws Exception {
        zkServer.stop();
    }

    @Test
    @CaptureSystemOutput
    public void processConnectedTest(CaptureSystemOutput.OutputCapture outputCapture) throws IOException, InterruptedException {
        ServiceRegistry registry = new ServiceRegistry(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT);
        outputCapture.expect(containsStringIgnoringCase("Successfully connected to Zookeeper"));
        registry.connectToZookeeper();
        Thread.sleep(SLEEP_INTERVAL);
        registry.close();
    }

    // Check if ServiceRegistry disconnects from zookeeper and prints out a disconnected message
    @Test
    @CaptureSystemOutput
    public void processDisconnectedTest(CaptureSystemOutput.OutputCapture outputCapture) throws IOException, InterruptedException {
        ServiceRegistry registry = new ServiceRegistry(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT);
        registry.connectToZookeeper();
        outputCapture.expect(containsStringIgnoringCase("disconnected"));
        registry.close();
        Thread.sleep(SLEEP_INTERVAL);
    }

    // Creates the /services parent znode
    @Test
    void initialiseCreateServicesZnodeTest() throws KeeperException, InterruptedException, IOException {
        ServiceRegistry registry = new ServiceRegistry(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT);
        registry.connectToZookeeper();
        registry.initialise();
        Stat result = zooKeeper.exists(SERVICES_PARENT_ZNODE, false);

        assertNotNull(result, "/services parent znode wasn't created.");
        registry.close();
    }

    // Check if /workers znode is of the correct type
    @Test
    void initialiseCreateServicesZnodeModeTest() throws KeeperException, InterruptedException, IOException {
        ServiceRegistry registry = new ServiceRegistry(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT);
        registry.connectToZookeeper();
        registry.initialise();
        Stat result = zooKeeper.exists(SERVICES_PARENT_ZNODE, false);
        assertEquals(0, result.getEphemeralOwner(), "/services parent znode is of the wrong type");
        registry.close();
    }

    // Doesn't create /services parent znode if it already exists
    @Test
    void initialiseCreateServicesZnodeAlreadyExistsTest() throws KeeperException, InterruptedException, IOException {
        ServiceRegistry registry = new ServiceRegistry(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT);
        registry.connectToZookeeper();
        registry.initialise();
        Stat result = zooKeeper.exists(SERVICES_PARENT_ZNODE, false);
        Assertions.assertAll(
                () -> assertNotNull(result, "/services parent znode wasn't created."),
                () -> assertDoesNotThrow(registry::initialise, "Shouldn't try to create /workers parent znode if it already exists")
        );
        registry.close();
    }

    @Test
    void initialiseServiceRegistryGetsExistingServicesTest() throws IOException, InterruptedException, KeeperException {
        helperCreateServicesParentZnode();
        Set<Map.Entry<String, String>> testServices = new HashSet<>();
        testServices.add(new AbstractMap.SimpleEntry<>("service1", "host1:1234"));
        testServices.add(new AbstractMap.SimpleEntry<>("service2", "host2:1234"));

        helperCreateServicesChildZnodes(testServices);

        ServiceRegistry registry = new ServiceRegistry(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT);
        registry.connectToZookeeper();
        registry.initialise();

        Map<String, String> registeredServices = registry.getServices();

        assertEquals(2, registeredServices.size());
        assertTrue(registeredServices.entrySet().containsAll(testServices));
    }

    @Test
    void firstNewServiceAddedTest() throws InterruptedException, KeeperException, IOException {
//        helperCreateServicesParentZnode();
        ServiceRegistry registry = new ServiceRegistry(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT);
        registry.connectToZookeeper();
        registry.initialise();

        // No services listed initially
        Map<String, String> registeredServices = registry.getServices();
        assertEquals(0, registeredServices.size());

        // Add a new service
        Set<Map.Entry<String, String>> testServices = new HashSet<>();
        testServices.add(new AbstractMap.SimpleEntry<>("service1", "host1:2345"));
        helperCreateServicesChildZnodes(testServices);

        // Service should now be listed
        Thread.sleep(SLEEP_INTERVAL);
        registeredServices = registry.getServices();
        assertEquals(1, registeredServices.size());
        assertTrue(registeredServices.entrySet().containsAll(testServices));
    }

    @Test
    void secondNewServiceAddedTest() throws InterruptedException, KeeperException, IOException {
        ServiceRegistry registry = new ServiceRegistry(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT);
        registry.connectToZookeeper();
        registry.initialise();

        // One services listed initially
        Map<String, String> registeredServices = registry.getServices();
        assertEquals(0, registeredServices.size());

        // Add a new service
        Set<Map.Entry<String, String>> testServices = new HashSet<>();
        testServices.add(new AbstractMap.SimpleEntry<>("service1", "host1:2345"));
        helperCreateServicesChildZnodes(testServices);

        // Service should now be listed
        Thread.sleep(SLEEP_INTERVAL);
        registeredServices = registry.getServices();
        assertEquals(1, registeredServices.size());
        assertTrue(registeredServices.entrySet().containsAll(testServices));

        // Add another new service
        Set<Map.Entry<String, String>> secondTestService = new HashSet<>();
        secondTestService.add(new AbstractMap.SimpleEntry<>("service2", "host1:6789"));
        helperCreateServicesChildZnodes(secondTestService);

        // Check if new service was added
        Thread.sleep(SLEEP_INTERVAL);
        registeredServices = registry.getServices();
        assertEquals(2, registeredServices.size());
        assertTrue(registeredServices.entrySet().containsAll(secondTestService));
    }

    @Test
    void serviceRemovedTest() throws InterruptedException, KeeperException, IOException {
        ServiceRegistry registry = new ServiceRegistry(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT);
        registry.connectToZookeeper();
        registry.initialise();

        // Add a new service
        Set<Map.Entry<String, String>> testServices = new HashSet<>();
        testServices.add(new AbstractMap.SimpleEntry<>("service1", "host1:2345"));
        helperCreateServicesChildZnodes(testServices);

        // Service now be listed
        Thread.sleep(SLEEP_INTERVAL);
        Map<String, String> registeredServices = registry.getServices();
        assertEquals(1, registeredServices.size());
        assertTrue(registeredServices.entrySet().containsAll(testServices));

        // Remove service
        helperDeleteZnode("service1");

        // Service should no longer be listed
        Thread.sleep(SLEEP_INTERVAL);
        registeredServices = registry.getServices();
        assertEquals(0, registeredServices.size());
    }

    @Test
    void serviceAddressChangedTest() throws InterruptedException, KeeperException, IOException {
        // Add a new service
        String testServiceName = "service1";
        String testServiceAddress = "host1:2345";
        helperCreateServicesParentZnode();
        Set<Map.Entry<String, String>> testServices = new HashSet<>();
        testServices.add(new AbstractMap.SimpleEntry<>(testServiceName, testServiceAddress));
        helperCreateServicesChildZnodes(testServices);

        ServiceRegistry registry = new ServiceRegistry(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT);
        registry.connectToZookeeper();
        registry.initialise();

        // Service should be listed
        Map<String, String> registeredServices = registry.getServices();
        assertEquals(1, registeredServices.size());
        assertTrue(registeredServices.entrySet().containsAll(testServices));

        // Change service addres
        String newTestServiceAddress = "host2:6789";
        helperUpdateZnode(testServiceName, newTestServiceAddress);

        // Service should have correct addres
        Thread.sleep(SLEEP_INTERVAL);
        registeredServices = registry.getServices();
        assertEquals(newTestServiceAddress, registeredServices.get(testServiceName));
    }

    private void helperUpdateZnode(String testServiceName, String newTestServiceAddress) throws InterruptedException, KeeperException {
        zooKeeper.setData(getFullPath(testServiceName), newTestServiceAddress.getBytes(StandardCharsets.UTF_8), -1);
    }

    @Test
    void registerServiceCreatesZNodeTest() throws IOException, InterruptedException, KeeperException {
        ServiceRegistry registry = new ServiceRegistry(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT);
        registry.connectToZookeeper();
        registry.initialise();

        String testServiceName = "testService";
        String testServiceAddress = "testhost:1234";

        registry.register(testServiceName, testServiceAddress);

        Stat stat = zooKeeper.exists(SERVICES_PARENT_ZNODE + "/" + testServiceName, false);

        assertNotNull(stat, "register failed to create znode");
    }

    @Test
    void registerServiceCorrectDataTest() throws IOException, InterruptedException, KeeperException {
        ServiceRegistry registry = new ServiceRegistry(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT);
        registry.connectToZookeeper();
        registry.initialise();

        String testServiceName = "testService";
        String testServiceAddress = "testhost:1234";
        String testServicePath = getFullPath(testServiceName);

        registry.register(testServiceName, testServiceAddress);

        Stat stat = zooKeeper.exists(testServicePath, false);
        assertNotNull(stat, "register failed to create znode");

        byte[] data = zooKeeper.getData(testServicePath, false, null);
        assertArrayEquals(testServiceAddress.getBytes(), data, "Service address is incorrectly stored");
    }

    @Test
    void registerServiceCorrectModeTest() throws IOException, InterruptedException, KeeperException {
        ServiceRegistry registry = new ServiceRegistry(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT);
        registry.connectToZookeeper();
        registry.initialise();

        String testServiceName = "testService";
        String testServiceAddress = "testhost:1234";
        String testServicePath = getFullPath(testServiceName);

        registry.register(testServiceName, testServiceAddress);

        Stat result = zooKeeper.exists(testServicePath, false);
        assertNotEquals(0, result.getEphemeralOwner(), "registered znode is of the wrong type");
        registry.close();
    }

    private String getFullPath(String znodeName) {
        return SERVICES_PARENT_ZNODE + "/" + znodeName;
    }
    void helperCreateServicesParentZnode() throws KeeperException, InterruptedException {
        helperCreateZnode(SERVICES_PARENT_ZNODE, new byte[]{}, CreateMode.PERSISTENT);
    }

    void helperCreateServicesChildZnodes(Set<Map.Entry<String, String>> servicesChildZnodes)
            throws KeeperException, InterruptedException {
        for (Map.Entry<String, String> service : servicesChildZnodes) {
            helperCreateZnode(
                    SERVICES_PARENT_ZNODE + "/" + service.getKey(),
                    service.getValue().getBytes(StandardCharsets.UTF_8),
                    CreateMode.EPHEMERAL);
        }
    }

    void helperCreateEphemeralZnode(String path, byte[] data) throws InterruptedException, KeeperException {
        helperCreateZnode(path, data, CreateMode.EPHEMERAL);
    }

    void helperCreateZnode(String path, byte[] data, CreateMode mode) throws KeeperException, InterruptedException {
        zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
    }

    void helperDeleteZnode(String path) throws InterruptedException, KeeperException {
        zooKeeper.delete(SERVICES_PARENT_ZNODE + "/" + path, -1);
    }
}