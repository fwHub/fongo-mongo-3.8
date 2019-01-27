package com.github.fakemongo.integration;

import com.github.fakemongo.junit.FongoRule;
import com.google.common.collect.Sets;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.UpdateResult;
import org.assertj.core.api.Assertions;
import org.bson.Document;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.query.Update;

import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

/**
 * User: william
 * Date: 15/03/14
 */
public class SpringMongoOperationTest {

  @Rule
  public FongoRule fongoRule = new FongoRule(false);

  private MongoOperations mongoOperations;

  @Before
  public void before() throws Exception {
    MongoClient mongoClient = fongoRule.getMongoClient();
    //Mongo mongo = new MongoClient();
    mongoOperations = new MongoTemplate(new SimpleMongoDbFactory(mongoClient, UUID.randomUUID().toString()));
  }

  @Test
  public void insertAndIndexesTest() {
    Item item = new Item(UUID.randomUUID(), "name", new Date());
    mongoOperations.insert(item);

    MongoCollection<Document> collection = mongoOperations.getCollection(Item.COLLECTION_NAME);
    assertEquals(1, collection.count());

    IndexOperations indexOperations = mongoOperations.indexOps(Item.COLLECTION_NAME);
    System.out.println(indexOperations.getIndexInfo());
    boolean indexedId = false;
    boolean indexedName = false;
    for (IndexInfo indexInfo : indexOperations.getIndexInfo()) {
      if (indexInfo.isIndexForFields(Collections.singletonList("_id"))) {
        indexedId = true;
      }
      if (indexInfo.isIndexForFields(Collections.singletonList("name"))) {
        indexedName = true;
      }
    }
    Assertions.assertThat(indexedId).as("_id field is not indexedId").isTrue();
    Assertions.assertThat(indexedName).as("name field is not indexedId").isTrue();
  }


  static class A {
    public A(String id, Set<B> bs) {
      this.id = id;
      this.bs = bs;
    }

    String id;
    Set<B> bs;
  }

  static class B {
    public B(String reference, Set<String> ids) {
      this.reference = reference;
      this.ids = ids;
    }

    String reference;
    Set<String> ids;
  }

  @Test
  public void test_ids() {
    // Given
    B b = new B("ref", Sets.newHashSet("1", "2"));
    B b2 = new B("ref2", null);
    A a = new A("id", Sets.newHashSet(b, b2));
    mongoOperations.insert(a);
    String reference = "ref";
    String idToPull = null;

    // When
    final UpdateResult writeResult = mongoOperations.updateFirst(query(where("_id").is(a.id).and("bs").elemMatch(where("reference").is(reference))), new Update().pull("bs.$.ids", idToPull), A.class);

    // Then
    Assertions.assertThat(writeResult.getModifiedCount()).isEqualTo(1);
  }

}
