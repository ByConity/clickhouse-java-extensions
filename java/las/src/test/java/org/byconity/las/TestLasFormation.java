package org.byconity.las;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;


public class TestLasFormation {

    static String getAk() {
        return System.getenv("LAS_ACCESS_KEY");
    }

    static String getSk() {
        return System.getenv("LAS_SECRET_KEY");
    }

    static String getMetastoreUrl() {
        return System.getenv("LAS_URL");
    }

    @Ignore
    @Test
    public void testGetTable() throws TException {
        Map<String, String> params = new HashMap<>();
        params.put("access_key", getAk());
        params.put("secret_key", getSk());
        params.put("region", "cn-beijing");
        params.put("endpoint", "180.184.41.234:48869");
        params.put("lf.metastore.catalog", "hive");

        params.put(HiveConf.ConfVars.METASTOREURIS.varname, getMetastoreUrl());
        LasFormationClient client = new LasFormationClient(params);

        Table tbl = client.getTableImpl("bhlastest", "tbl");

        List<Partition> partitions =
                client.getPartitionsByFilterImpl("bhlastest", "tbl", "", (short) -1);
        for (Partition p : partitions) {
            System.out.println(p.toString());
        }

    }

    @Ignore
    @Test
    public void testGetAllDbs() throws TException {
        Map<String, String> params = new HashMap<>();
        params.put("access_key", getAk());
        params.put("secret_key", getSk());
        params.put("region", "cn-beijing");
        params.put("lf.metastore.catalog", "catalog_test");

        params.put(HiveConf.ConfVars.METASTOREURIS.varname, getMetastoreUrl());
        LasFormationClient client = new LasFormationClient(params);

        String[] dbs = client.listDatabases();

        for (String p : dbs) {
            System.out.println(p);
        }

        String[] tables = client.listTables("las_db");
        for (String p : tables) {
            System.out.println(p);
        }

    }

    class SimpleRunnable implements Runnable {
        private final Map<String, String> params;
        private LasFormationClient client;

        SimpleRunnable(Map<String, String> params_) throws MetaException {
            this.params = params_;
            this.client = new LasFormationClient(params);
        }


        public void run() {
            Table tbl = null;
            try {
                tbl = client.getTableImpl("bhlastest", "tbl");
            } catch (TException e) {
                throw new RuntimeException(e);
            }

            List<Partition> partitions = null;
            try {
                partitions = client.getPartitionsByFilterImpl("bhlastest", "tbl", "", (short) -1);
            } catch (TException e) {
                throw new RuntimeException(e);
            }
            for (Partition p : partitions) {
                System.out.println(p.toString());
            }
        }
    }

    @Ignore
    @Test
    public void testMultiThread() throws TException {
        Logger LOG = LoggerFactory.getLogger(LasMetaClient.class);
        LOG.info("running testMultiThread");
        Map<String, String> params = new HashMap<>();
        params.put("access_key", getAk());
        params.put("secret_key", getSk());
        params.put("region", "cn-beijing");
        params.put("lf.metastore.catalog", "");
        params.put(HiveConf.ConfVars.METASTOREURIS.varname, getMetastoreUrl());
        ExecutorService executor = Executors.newFixedThreadPool(8);
        int numTasks = 20;
        try {
            for (int i = 0; i < numTasks; i++) {
                executor.execute(new SimpleRunnable(params));
            }
        } catch (Exception err) {
            err.printStackTrace();
        }
        executor.shutdown();
    }
}
