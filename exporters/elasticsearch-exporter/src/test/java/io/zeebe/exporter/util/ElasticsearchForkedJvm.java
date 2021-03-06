/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.exporter.util;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpHost;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.security.PutRoleRequest;
import org.elasticsearch.client.security.PutUserRequest;
import org.elasticsearch.client.security.RefreshPolicy;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.client.security.user.privileges.Role;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

public class ElasticsearchForkedJvm implements ElasticsearchNode<ElasticsearchForkedJvm> {

  private static final int DEFAULT_HTTP_PORT = 9200;
  private static final int DEFAULT_TCP_PORT = 9300;

  private EmbeddedElastic.Builder builder;
  private EmbeddedElastic elastic;
  private boolean isSslEnabled;
  private boolean isAuthEnabled;
  private String username;
  private String password;

  private final List<String> javaOptions = new ArrayList<>();

  public ElasticsearchForkedJvm() {
    this(EmbeddedElastic.builder());
    configure();
  }

  public ElasticsearchForkedJvm(EmbeddedElastic.Builder builder) {
    this.builder = builder;
  }

  protected void configure() {
    final String version = ElasticsearchClient.class.getPackage().getImplementationVersion();
    builder
        .withElasticVersion(version)
        .withSetting("discovery.type", "single-node")
        .withSetting(PopularProperties.CLUSTER_NAME, "zeebe")
        .withSetting(PopularProperties.HTTP_PORT, DEFAULT_HTTP_PORT)
        .withSetting(PopularProperties.TRANSPORT_TCP_PORT, DEFAULT_TCP_PORT)
        .withCleanInstallationDirectoryOnStop(true)
        .withStartTimeout(2, TimeUnit.MINUTES);
  }

  @Override
  public void start() {
    if (elastic == null) {
      elastic = builder.build();

      try {
        elastic.start();
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }

      if (isAuthEnabled) {
        setupUser();
      }
    }
  }

  @Override
  public void stop() {
    if (elastic != null) {
      elastic.stop();
      elastic = null;
      javaOptions.clear();
      isSslEnabled = false;
      builder = EmbeddedElastic.builder();
    }
  }

  @Override
  public ElasticsearchForkedJvm withXpack() {
    builder.withSetting("xpack.license.self_generated.type", "trial");
    return this;
  }

  @Override
  public ElasticsearchForkedJvm withUser(String username, String password) {
    withXpack();
    builder.withSetting("xpack.security.enabled", "true");

    // the following allow the "liveness" check of EmbeddedElastic to succeed since we can't give
    // it any credentials by enabling anonymous users to monitor the cluster health
    builder
        .withSetting("xpack.security.authc.anonymous.username", "anon")
        .withSetting("xpack.security.authc.anonymous.roles", "superuser")
        .withSetting("xpack.security.authc.anonymous.authz_exception", true);

    // allows us to use the default changeme password when setting up user/roles
    builder.withSetting("xpack.security.authc.accept_default_password", true);

    this.username = username;
    this.password = password;
    this.isAuthEnabled = true;

    return this;
  }

  @Override
  public ElasticsearchForkedJvm withJavaOptions(String... options) {
    javaOptions.addAll(Arrays.asList(options));
    builder.withEsJavaOpts(String.join(", ", javaOptions));

    return this;
  }

  @Override
  public ElasticsearchForkedJvm withKeyStore(String keyStore) {
    final Path keyStorePath;
    try {
      keyStorePath =
          Paths.get(getClass().getClassLoader().getResource(keyStore).toURI()).toAbsolutePath();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    withXpack();
    builder
        .withSetting("xpack.security.http.ssl.enabled", true)
        .withSetting("xpack.security.http.ssl.keystore.path", keyStorePath.toString());

    isSslEnabled = true;
    return this;
  }

  @Override
  public HttpHost getRestHttpHost() {
    final String scheme = isSslEnabled ? "https" : "http";
    final String host = "localhost";

    return new HttpHost(host, DEFAULT_HTTP_PORT, scheme);
  }

  private void setupUser() {
    final RestHighLevelClient client = newClient();
    final User user = new User(username, Collections.singleton("zeebe-exporter"));

    try {
      createRole(client);
      client
          .security()
          .putUser(
              PutUserRequest.withPassword(
                  user, password.toCharArray(), true, RefreshPolicy.IMMEDIATE),
              RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private RestHighLevelClient newClient() {
    final RestClientBuilder builder = RestClient.builder(getRestHttpHost());
    return new RestHighLevelClient(builder);
  }

  // note: caveat, do not use custom index prefixes!
  private void createRole(RestHighLevelClient client) throws IOException {
    final Role role = Role.builder().name("zeebe-exporter").build();

    client
        .security()
        .putRole(new PutRoleRequest(role, RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
  }
}
