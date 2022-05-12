package com.ccy.core;

import org.apache.flink.streaming.connectors.elasticsearch7.RestClientFactory;
import org.apache.http.Header;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;

public class RestClientFactoryImpl implements RestClientFactory {

    private static final Logger logger = LoggerFactory.getLogger(RestClientFactoryImpl.class);

    private String elasticUsername;
    private String elasticPassword;
    private String elasticCertificate;

    public RestClientFactoryImpl(String elastic_username, String elastic_password, String elastic_certificate) {
        this.elasticUsername = elastic_username;
        this.elasticPassword = elastic_password;
        this.elasticCertificate = elastic_certificate;
    }

    @Override
    public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
        Header[] headers = new BasicHeader[]{new BasicHeader("Content-Type", "application/json")};
        restClientBuilder.setDefaultHeaders(headers); //以数组的形式可以添加多个header
        restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {

            //设置用户密码
            CredentialsProvider provider = new BasicCredentialsProvider();
            provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(elasticUsername, elasticPassword));
            //ssl
//            Path caCertificatePath  = Paths.get("D:\\\\conf\\\\prs\\\\certs\\\\elasticsearch\\\\ca\\\\ca.crt");//新生成的keystore文件路径
            Path caCertificatePath = Paths.get(elasticCertificate);//新生成的KEYSTORE文件路径
            SSLContext sslContext = null;
            try {
                CertificateFactory factory = CertificateFactory.getInstance("X.509");
                Certificate trustedCa;
                try (InputStream is = Files.newInputStream(caCertificatePath)) {
                    trustedCa = factory.generateCertificate(is);
                }
                KeyStore trustStore = KeyStore.getInstance("pkcs12");
                trustStore.load(null, null);
                trustStore.setCertificateEntry("ca", trustedCa);
                SSLContextBuilder sslContextBuilder = SSLContexts.custom().loadTrustMaterial(trustStore, null);
                sslContext = sslContextBuilder.build();
            } catch (CertificateException | NoSuchAlgorithmException | KeyManagementException | KeyStoreException | IOException e) {
                logger.info(e.getMessage());
            }

            return httpClientBuilder.setDefaultCredentialsProvider(provider).setSSLContext(sslContext).setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
        });

    }
}
