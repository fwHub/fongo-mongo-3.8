package com.mongodb;

import com.github.fakemongo.Fongo;
import static com.mongodb.WriteConcern.REPLICA_ACKNOWLEDGED;
import com.mongodb.client.MongoDatabase;
import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.internal.FongoMongoDatabase;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.Before;
import org.junit.Test;

public class FongoMongoDatabaseTest {

  private FongoMongoDatabase instance;

  @Before
  public void before() {
    instance = new FongoMongoDatabase("test", new Fongo("test2"));
  }

  @Test
  public void withCodecRegistry_change_codec_and_good_instance() throws Exception {
    final CodecRegistry codecRegistry = MongoClient.getDefaultCodecRegistry();
    final MongoDatabase mongoDatabase = instance.withCodecRegistry(codecRegistry);

    assertThat(mongoDatabase).hasSameClassAs(instance);
    assertThat(mongoDatabase.getCodecRegistry()).isEqualTo(codecRegistry);
  }

  @Test
  public void withReadPreference_change_readPreference_and_good_instance() throws Exception {
    final MongoDatabase mongoDatabase = instance.withReadPreference(ReadPreference.nearest());

    assertThat(mongoDatabase).hasSameClassAs(instance);
    assertThat(mongoDatabase.getReadPreference()).isEqualTo(ReadPreference.nearest());
  }

  @Test
  public void withWriteConcern_change_writeConcern_and_good_instance() throws Exception {
    final MongoDatabase mongoDatabase = instance.withWriteConcern(REPLICA_ACKNOWLEDGED);

    assertThat(mongoDatabase).hasSameClassAs(instance);
    assertThat(mongoDatabase.getWriteConcern()).isEqualTo(REPLICA_ACKNOWLEDGED);
  }
}