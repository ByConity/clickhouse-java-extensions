package org.byconity.iceberg.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.byconity.common.util.S3Utils;

import java.util.List;

public class IcebergS3Demo {
    public static void main(String[] args) throws Exception {
        Configuration hadoopConf = createConfig();

        // Set the warehouse location to an S3 path
        String warehousePath = "tos://bhlasinfo/iceberg";
        HadoopCatalog catalog = new HadoopCatalog(hadoopConf, warehousePath);

        if (catalog.namespaceExists(Namespace.of("demo"))) {
            catalog.createNamespace(Namespace.of("demo"));
        }

        List<Namespace> namespaces = catalog.listNamespaces();
        System.out.println(namespaces);

        catalog.close();
    }

    private static Configuration createConfig() {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.s3a.access.key", "");
        hadoopConf.set("fs.s3a.secret.key", "");
        hadoopConf.set("fs.s3a.endpoint.region", "shanghai");
        hadoopConf.set("fs.s3a.endpoint", "tos-s3-cn-beijing.volces.com");
        hadoopConf.set("fs.s3a.path.style.access", "false");

        for (String supportedProtocol : S3Utils.SUPPORTED_PROTOCOLS) {
            hadoopConf.set(String.format("fs.%s.impl", supportedProtocol.toLowerCase()),
                    "org.apache.hadoop.fs.s3a.S3AFileSystem");
        }
        return hadoopConf;
    }
}
