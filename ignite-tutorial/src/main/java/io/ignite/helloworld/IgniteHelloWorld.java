package io.ignite.helloworld;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

import java.util.*;
import java.util.stream.Collectors;

public class IgniteHelloWorld {
    public static void main(String[] args) throws IgniteException {
       /* // Preparing IgniteConfiguration using Java APIs
        IgniteConfiguration cfg = new IgniteConfiguration();

        // The node will be started as a client node.
        cfg.setClientMode(true);

        // Classes of custom Java logic will be transferred over the wire from this app.
        cfg.setPeerClassLoadingEnabled(true);

        // Setting up an IP Finder to ensure the client can locate the servers.
        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));

        // Starting the node
        Ignite ignite = Ignition.start(cfg);

        // Create an IgniteCache and put some values in it.
        IgniteCache<Integer, Map<String,String>> cache = ignite.getOrCreateCache("myCache");
        Map<String,String> map = new HashMap<>();
        map.put("1","sk");
        map.put("2","pk");
        map.put("3","dk");
        map.put("4","gk");
        Map<String,String> map1 = new HashMap<>();
        map1.put("1","sp");
        map1.put("2","pp");
        map1.put("3","dp");
        map1.put("4","gp");
        cache.put(1, map);
        cache.put(2, map1);

        System.out.println(">> Created the cache and add the values.");

        // Executing custom Java compute task on server nodes.
        ignite.compute(ignite.cluster().forServers()).broadcast(new RemoteTask());

        System.out.println(">> Compute task is executed, check for output on the server nodes.");

        // Disconnect from the cluster.
        ignite.close();*/

        accessFromServer();

    }

    private static void accessFromServer() {
        // Preparing IgniteConfiguration using Java APIs
        IgniteConfiguration cfg = new IgniteConfiguration();

        // The node will be started as a client node.
        cfg.setClientMode(true);

        // Classes of custom Java logic will be transferred over the wire from this app.
        cfg.setPeerClassLoadingEnabled(true);

        // Setting up an IP Finder to ensure the client can locate the servers.
        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));

        // Starting the node
        Ignite ignite = Ignition.start(cfg);

        // Create an IgniteCache and put some values in it.
        IgniteCache<String,String> cache = ignite.getOrCreateCache("mycache");
        Set<String> set = new HashSet<>();
        System.out.println("accessing from server====> "+cache.get("1267157507463577603"));
        System.out.println("set values===>"+set);

        // Disconnect from the cluster.
        ignite.close();
    }

    /**
     * A compute tasks that prints out a node ID and some details about its OS and JRE.
     * Plus, the code shows how to access data stored in a cache from the compute task.
     */
    private static class RemoteTask implements IgniteRunnable {
        @IgniteInstanceResource
        Ignite ignite;

        @Override public void run() {
            System.out.println(">> Executing the compute task");

            System.out.println(
                    "   Node ID: " + ignite.cluster().localNode().id() + "\n" +
                            "   OS: " + System.getProperty("os.name") +
                            "   JRE: " + System.getProperty("java.runtime.name"));

            IgniteCache<Integer, Map<String,String>> cache = ignite.cache("myCache");

            List<String> collect = cache.get(1).values()
                    .stream()
                    .collect(Collectors.toList());

            List<String> collect1 = cache.get(2).values()
                    .stream()
                    .collect(Collectors.toList());

            collect.addAll(collect1);
            System.out.println(collect);
        }
    }
}
