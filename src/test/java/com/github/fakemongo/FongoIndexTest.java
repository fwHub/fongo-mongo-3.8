package com.github.fakemongo;

import com.github.fakemongo.impl.Util;
import com.github.fakemongo.impl.index.IndexAbstract;
import com.github.fakemongo.junit.FongoRule;
import com.google.common.collect.Lists;
import com.mongodb.*;
import org.assertj.core.api.Assertions;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

public class FongoIndexTest {

  public final FongoRule fongoRule = new FongoRule("db", !true);

  public final ExpectedException exception = ExpectedException.none();

  @Rule
  public RuleChain ruleChain = RuleChain.outerRule(exception).around(fongoRule);

  static IndexAbstract getIndex(DBCollection collection, String name) {
    FongoDBCollection fongoDBCollection = (FongoDBCollection) collection;

    IndexAbstract index = null;
    for (IndexAbstract i : fongoDBCollection.getIndexes()) {
      if (i.getName().equals(name)) {
        index = i;
        break;
      }
    }
    assertNotNull("index not found :" + name, index);
    return index;
  }

  @Test
  public void testCreateIndexes() {
    DBCollection collection = fongoRule.newCollection("coll");
    collection.createIndex(new BasicDBObject("n", 1));
    collection.createIndex(new BasicDBObject("b", 1));
    List<DBObject> indexes = collection.getIndexInfo();
    assertEquals(
        Arrays.asList(
            new BasicDBObject("v", 1).append("key", new BasicDBObject("_id", 1)).append("name", "_id_").append("ns", "db.coll"),
            new BasicDBObject("v", 1).append("key", new BasicDBObject("n", 1)).append("name", "n_1").append("ns", "db.coll"),
            new BasicDBObject("v", 1).append("key", new BasicDBObject("b", 1)).append("name", "b_1").append("ns", "db.coll")
        ), indexes
    );
  }

  /**
   * Same index = do not recreate.
   */
  @Test
  public void testCreateSameIndex() {
    DBCollection collection = fongoRule.newCollection("coll");
    collection.createIndex(new BasicDBObject("n", 1));
    collection.createIndex(new BasicDBObject("n", 1));
    List<DBObject> indexes = collection.getIndexInfo();
    assertEquals(
        Arrays.asList(
            new BasicDBObject("v", 1).append("key", new BasicDBObject("_id", 1)).append("name", "_id_").append("ns", "db.coll"),
            new BasicDBObject("v", 1).append("key", new BasicDBObject("n", 1)).append("name", "n_1").append("ns", "db.coll")
        ), indexes
    );
  }

  /**
   * Same index = do not recreate.
   */
  @Test
  public void testCreateSameIndexWithDifferentsOptions() {
    DBCollection collection = fongoRule.newCollection("coll");
    collection.createIndex(new BasicDBObject("n", 1), "n_1");

    //com.mongodb.MongoCommandException: Command failed with error 85: 'Index with name: n_1 already exists with
    // different options' on server 127.0.0.1:27017. The full response is
    // { "ok" : 0.0, "errmsg" : "Index with name: n_1 already exists with different options", "code" : 85 }
    ExpectedMongoException.expect(exception, MongoCommandException.class);
    ExpectedMongoException.expectCode(exception, 85);
    collection.createIndex(new BasicDBObject("n", 1), "n_1", true);
  }

  @Test
  public void testCreateIndexOnSameFieldInversedOrder() {
    DBCollection collection = fongoRule.newCollection("coll");
    collection.createIndex(new BasicDBObject("n", 1));
    collection.createIndex(new BasicDBObject("n", -1));
    List<DBObject> indexes = collection.getIndexInfo();
    assertEquals(
        Arrays.asList(
            new BasicDBObject("v", 1).append("key", new BasicDBObject("_id", 1)).append("name", "_id_").append("ns", "db.coll"),
            new BasicDBObject("v", 1).append("key", new BasicDBObject("n", 1)).append("name", "n_1").append("ns", "db.coll"),
            new BasicDBObject("v", 1).append("key", new BasicDBObject("n", -1)).append("name", "n_-1").append("ns", "db.coll")
        ), indexes
    );
  }

  @Test
  public void testDropIndexOnSameFieldInversedOrder() {
    assumeFalse(fongoRule.isRealMongo());
    DBCollection collection = fongoRule.newCollection("coll");
    collection.createIndex(new BasicDBObject("n", 1));
    collection.createIndex(new BasicDBObject("n", -1));
    List<DBObject> indexes = collection.getIndexInfo();
    assertEquals(
        Arrays.asList(
            new BasicDBObject("v", 1).append("key", new BasicDBObject("_id", 1)).append("name", "_id_").append("ns", "db.coll"),
            new BasicDBObject("v", 1).append("key", new BasicDBObject("n", 1)).append("name", "n_1").append("ns", "db.coll"),
            new BasicDBObject("v", 1).append("key", new BasicDBObject("n", -1)).append("name", "n_-1").append("ns", "db.coll")
        ), indexes
    );
    IndexAbstract index = getIndex(collection, "n_1");
    index = getIndex(collection, "n_-1");
  }

  @Test
  public void testDropIndex() {
    DBCollection collection = fongoRule.newCollection("coll");
    collection.createIndex(new BasicDBObject("n", 1));
    collection.createIndex(new BasicDBObject("b", 1));

    List<DBObject> indexes = collection.getIndexInfo();
    assertEquals(
        Arrays.asList(
            new BasicDBObject("v", 1).append("key", new BasicDBObject("_id", 1)).append("ns", "db.coll").append("name", "_id_"),
            new BasicDBObject("v", 1).append("key", new BasicDBObject("n", 1)).append("ns", "db.coll").append("name", "n_1"),
            new BasicDBObject("v", 1).append("key", new BasicDBObject("b", 1)).append("ns", "db.coll").append("name", "b_1")
        ), indexes
    );

    collection.dropIndex("n_1");
    indexes = collection.getIndexInfo();
    assertEquals(
        Arrays.asList(
            new BasicDBObject("v", 1).append("key", new BasicDBObject("_id", 1)).append("ns", "db.coll").append("name", "_id_"),
            new BasicDBObject("v", 1).append("key", new BasicDBObject("b", 1)).append("ns", "db.coll").append("name", "b_1")
        ), indexes
    );
  }

  @Test
  public void testDropIndexes() {
    DBCollection collection = fongoRule.newCollection("coll");
    collection.createIndex("n");
    collection.createIndex("b");

    List<DBObject> indexes = collection.getIndexInfo();
    assertEquals(
        Arrays.asList(
            new BasicDBObject("v", 1).append("key", new BasicDBObject("_id", 1)).append("name", "_id_").append("ns", "db.coll"),
            new BasicDBObject("v", 1).append("key", new BasicDBObject("n", 1)).append("name", "n_1").append("ns", "db.coll"),
            new BasicDBObject("v", 1).append("key", new BasicDBObject("b", 1)).append("name", "b_1").append("ns", "db.coll")
        ), indexes
    );

    collection.dropIndexes();
    indexes = collection.getIndexInfo();
    assertEquals(1, indexes.size());
  }

  @Test
  public void should_dropIndex_on_unexpected_index_throw_exception() {
    DBCollection collection = fongoRule.newCollection("coll");
    collection.createIndex("n");

    List<DBObject> indexes = collection.getIndexInfo();
    assertEquals(
        Arrays.asList(
            new BasicDBObject("v", 1).append("key", new BasicDBObject("_id", 1)).append("ns", "db.coll").append("name", "_id_"),
            new BasicDBObject("v", 1).append("key", new BasicDBObject("n", 1)).append("ns", "db.coll").append("name", "n_1")
        ), indexes
    );

    collection.dropIndex(new BasicDBObject("n", 1));
    exception.expectMessage("index not found with name [n_1]");
    collection.dropIndex(new BasicDBObject("n", 1));
  }

  // Data are already here, but duplicated.
  @Test
  public void testCreateIndexOnDuplicatedData() {
    DBCollection collection = fongoRule.newCollection();

    collection.insert(new BasicDBObject("n", 1));
    collection.insert(new BasicDBObject("n", 1));
    try {
      collection.createIndex(new BasicDBObject("n", 1), "n_1", true);
      fail("need MongoException on duplicate key.");
    } catch (MongoException me) {
      assertEquals(11000, me.getCode());
//      Assertions.assertThat(me.getMessage()).contains("E11000 duplicate key error index: " + collection.getFullName() + ".$n_1  dup key: { : [[1]] }");// TODO [[ instead of " : \"1\""
    }
  }

  /**
   * Try to update an object and doesn't violate the unique index.
   */
  @Test
  public void testUpdateObjectOnUniqueIndexSameValue() {
    DBCollection collection = fongoRule.newCollection();
    collection.createIndex(new BasicDBObject("n", 1), "n_1", true);

    collection.insert(new BasicDBObject("n", 1));
    collection.insert(new BasicDBObject("n", 2));

    // Update same.
    collection.update(new BasicDBObject("n", 2), new BasicDBObject("n", 2));

    assertEquals(1, collection.count(new BasicDBObject("n", 2)));
  }

  /**
   * Try to update an object and doesn't violate the unique index.
   */
  @Test
  public void testUpdateObjectOnUniqueIndexDifferentValue() {
    DBCollection collection = fongoRule.newCollection();
    collection.createIndex(new BasicDBObject("n", 1), "n_1", true);

    collection.insert(new BasicDBObject("n", 1));
    collection.insert(new BasicDBObject("n", 2));

    // Update same.
    collection.update(new BasicDBObject("n", 2), new BasicDBObject("n", 3));

    assertEquals(0, collection.count(new BasicDBObject("n", 2)));
    assertEquals(1, collection.count(new BasicDBObject("n", 3)));
  }

  /**
   * Try to update an object but with same value as existing.
   */
  @Test
  public void testUpdateObjectOnUniqueIndex() {
    DBCollection collection = fongoRule.newCollection();
    collection.createIndex(new BasicDBObject("n", 1), "n_1", true);

    collection.insert(new BasicDBObject("n", 1));
    collection.insert(new BasicDBObject("n", 2));

    // Update same.
    try {
      collection.update(new BasicDBObject("n", 2), new BasicDBObject("n", 1));
      fail("Must send MongoException");
    } catch (MongoException me) {
      assertEquals(11000, me.getCode());
    }

    assertEquals(1, collection.count(new BasicDBObject("n", 2)));
    assertEquals(1, collection.count(new BasicDBObject("n", 1)));
  }

  /**
   * Try to update an object but with same value as existing.
   */
  @Test
  public void testUpdateObjectOnUniqueIndexWithCreatingWithOption() {
    DBCollection collection = fongoRule.newCollection();
    collection.createIndex(new BasicDBObject("n", 1), new BasicDBObject("unique", 1));

    collection.insert(new BasicDBObject("n", 1));
    collection.insert(new BasicDBObject("n", 2));

    // Update same.
    try {
      collection.update(new BasicDBObject("n", 2), new BasicDBObject("n", 1));
      fail("Must send MongoException");
    } catch (MongoException me) {
      assertEquals(11000, me.getCode());
    }

    assertEquals(1, collection.count(new BasicDBObject("n", 2)));
    assertEquals(1, collection.count(new BasicDBObject("n", 1)));
  }

  @Test
  public void uniqueIndexesShouldNotPermitCreateOfDuplicatedEntries() {
    DBCollection collection = fongoRule.newCollection();

    collection.createIndex(new BasicDBObject("date", 1), new BasicDBObject("name", "uniqueDate").append("unique", true));

    // Insert
    collection.insert(new BasicDBObject("date", 1));
    collection.insert(new BasicDBObject("date", 2));
    try {
      collection.insert(new BasicDBObject("date", 1));
    } catch (MongoException me) {
      me.printStackTrace();
      assertEquals(11000, me.getCode()); // TODO diff from mongo, need 11000
    }
  }

  @Test
  public void indexesShouldBeRemoved() {
    DBCollection collection = fongoRule.newCollection();

    collection.createIndex(new BasicDBObject("date", 1));
    collection.createIndex(new BasicDBObject("field", 1), "fieldIndex");
    collection.createIndex(new BasicDBObject("string", 1), "stringIndex", true);

    List<DBObject> indexInfos = collection.getIndexInfo();
    assertEquals(4, indexInfos.size());
    assertEquals("_id_", indexInfos.get(0).get("name"));
    assertEquals("date_1", indexInfos.get(1).get("name"));
    assertEquals("fieldIndex", indexInfos.get(2).get("name"));
    assertEquals("stringIndex", indexInfos.get(3).get("name"));

    collection.dropIndex("fieldIndex");
    indexInfos = collection.getIndexInfo();
    assertEquals(3, indexInfos.size());
    assertEquals("_id_", indexInfos.get(0).get("name"));
    assertEquals("date_1", indexInfos.get(1).get("name"));
    assertEquals("stringIndex", indexInfos.get(2).get("name"));
  }

  @Test
  public void indexesMustBeUsedForFind() {
    assumeFalse(fongoRule.isRealMongo());
    DBCollection collection = fongoRule.newCollection();

    collection.createIndex(new BasicDBObject("firstname", 1).append("lastname", 1));
    collection.createIndex(new BasicDBObject("date", 1));
    collection.createIndex(new BasicDBObject("permalink", 1), new BasicDBObject("name", "permalink_1").append("unique", true));

    for (int i = 0; i < 20; i++) {
      collection.insert(new BasicDBObject("firstname", "firstname" + i % 10).append("lastname", "lastname" + i % 10).append("date", i % 15).append("permalink", i));
    }

    IndexAbstract indexFLname = getIndex(collection, "firstname_1_lastname_1");
    IndexAbstract indexDate = getIndex(collection, "date_1");
    IndexAbstract indexPermalink = getIndex(collection, "permalink_1");

    assertEquals(0, indexFLname.getLookupCount());
    assertEquals(0, indexDate.getLookupCount());
    assertEquals(0, indexPermalink.getLookupCount());

    collection.find(new BasicDBObject("firstname", "firstname0"));
    // No index used.
    assertEquals(0, indexFLname.getLookupCount());
    assertEquals(0, indexDate.getLookupCount());
    assertEquals(0, indexPermalink.getLookupCount());

    List<DBObject> objects = collection.find(new BasicDBObject("firstname", "firstname0").append("lastname", "lastname0")).toArray();
    assertEquals(2, objects.size());
    assertEquals(1, indexFLname.getLookupCount());
    assertEquals(0, indexDate.getLookupCount());
    assertEquals(0, indexPermalink.getLookupCount());

    objects = collection.find(new BasicDBObject("firstname", "firstname0").append("lastname", "lastname0").append("date", 0)).toArray();
    assertEquals(1, objects.size());
    assertEquals(2, indexFLname.getLookupCount());
    assertEquals(0, indexDate.getLookupCount());
    assertEquals(0, indexPermalink.getLookupCount());

    objects = collection.find(new BasicDBObject("permalink", 0)).toArray();
    assertEquals(1, objects.size());
    assertEquals(2, indexFLname.getLookupCount());
    assertEquals(0, indexDate.getLookupCount());
    assertEquals(1, indexPermalink.getLookupCount());
  }

  // Check if index is correctly cleaned.
  @Test
  public void afterRemoveObjectMustNotBeRetrieved() {
    DBCollection collection = fongoRule.newCollection();

    collection.createIndex(new BasicDBObject("date", 1));

    collection.insert(new BasicDBObject("date", 1));

    List<DBObject> result = collection.find(new BasicDBObject("date", 1)).toArray();
    assertEquals(1, result.size());
    collection.remove(new BasicDBObject("date", 1));
    result = collection.find(new BasicDBObject("date", 1)).toArray();
    assertEquals(0, result.size());
  }

  @Test
  public void uniqueIndexesShouldNotPermitUpdateOfDuplicatedEntriesWhenUpdatedById() {
    DBCollection collection = fongoRule.newCollection();

    collection.createIndex(new BasicDBObject("date", 1), new BasicDBObject("name", "uniqueDate").append("unique", true));

    // Insert
    collection.insert(new BasicDBObject("_id", 1).append("date", 1));
    collection.insert(new BasicDBObject("_id", 2).append("date", 2));

    try {
      collection.update(new BasicDBObject("_id", 2), new BasicDBObject("date", 1));
      fail("should throw MongoException");
    } catch (MongoException me) {
      assertEquals(11000, me.getCode());
    }

    // Verify object is NOT modify
    assertEquals(1, collection.count(new BasicDBObject("_id", 2)));
    assertEquals(2, collection.find(new BasicDBObject("_id", 2)).next().get("date"));
  }

  @Test
  public void uniqueIndexesShouldNotPermitCreateOfDuplicatedEntriesWhenUpdatedByField() {
    DBCollection collection = fongoRule.newCollection();

    collection.createIndex(new BasicDBObject("date", 1), "uniqueDate", true);

    // Insert
    collection.insert(new BasicDBObject("_id", 1).append("date", 1));
    collection.insert(new BasicDBObject("_id", 2).append("date", 2));

    try {
      collection.update(new BasicDBObject("date", 2), new BasicDBObject("date", 1));
      fail("should throw MongoException");
    } catch (MongoException me) {
      assertEquals(11000, me.getCode());
    }

    // Verify object is NOT modify
    assertEquals(2, collection.find(new BasicDBObject("_id", 2)).next().get("date"));
  }

  @Test
  public void uniqueIndexesCanPermitUpdateOfDuplicatedEntriesWhenUpdatedByIdTheSameObject() {
    DBCollection collection = fongoRule.newCollection();

    collection.createIndex(new BasicDBObject("date", 1), new BasicDBObject("name", "uniqueDate").append("unique", true));

    // Insert
    collection.insert(new BasicDBObject("_id", 1).append("date", 1));
    collection.insert(new BasicDBObject("_id", 2).append("date", 2));

    // Test
    collection.update(new BasicDBObject("_id", 2), new BasicDBObject("date", 2));

    // Verify object is NOT modified
    assertEquals(2, collection.find(new BasicDBObject("_id", 2)).next().get("date"));
  }

  @Test
  public void uniqueIndexesCanPermitCreateOfDuplicatedEntriesWhenUpdatedByFieldTheSameObject() {
    DBCollection collection = fongoRule.newCollection();

    collection.createIndex(new BasicDBObject("date", 1), "uniqueDate", true);

    // Insert
    collection.insert(new BasicDBObject("_id", 1).append("date", 1));
    collection.insert(new BasicDBObject("_id", 2).append("date", 2));
    collection.update(new BasicDBObject("date", 1), new BasicDBObject("date", 1));

    // Verify object is NOT modify
    assertEquals(1, collection.find(new BasicDBObject("_id", 1)).next().get("date"));
  }

  @Test
  public void uniqueIndexesShouldPermitCreateOfDuplicatedEntriesWhenIndexIsRemoved() {
    DBCollection collection = fongoRule.newCollection();

    collection.createIndex(new BasicDBObject("date", 1), "uniqueDate", true);

    // Insert
    collection.insert(new BasicDBObject("_id", 1).append("date", 1));
    collection.insert(new BasicDBObject("_id", 2).append("date", 2));

    collection.dropIndex("uniqueDate");

    collection.update(new BasicDBObject("_id", 2), new BasicDBObject("date", 1));
    collection.insert(new BasicDBObject("_id", 3).append("date", 1));
  }

  @Test
  public void uniqueIndexesShouldPermitCreateOfDuplicatedEntriesWhenAllIndexesAreRemoved() {
    DBCollection collection = fongoRule.newCollection();

    collection.createIndex(new BasicDBObject("date", 1), new BasicDBObject("name", "uniqueDate").append("unique", true));

    // Insert
    collection.insert(new BasicDBObject("_id", 1).append("date", 1));
    collection.insert(new BasicDBObject("_id", 2).append("date", 2));

    collection.dropIndex("uniqueDate");

    collection.update(new BasicDBObject("_id", 2), new BasicDBObject("date", 1));
    collection.insert(new BasicDBObject("_id", 3).append("date", 1));
  }

  // Add or remove a field in an object must populate the index.
  @Test
  public void updateAndAddFieldMustAddIntoIndex() {
    assumeFalse(fongoRule.isRealMongo());
    DBCollection collection = fongoRule.newCollection();

    collection.createIndex(new BasicDBObject("date", 1));

    // Insert
    collection.insert(new BasicDBObject("_id", 2));

    IndexAbstract index = getIndex(collection, "date_1");
    assertEquals(0, index.size());

    collection.update(new BasicDBObject("_id", 2), new BasicDBObject("date", 1));
    assertEquals(1, index.size());
  }

  // Add or remove a field in an object must populate the index.
  @Test
  public void updateAndRemoveFieldMustAddIntoIndex() {
    assumeFalse(fongoRule.isRealMongo());
    DBCollection collection = fongoRule.newCollection();

    collection.createIndex(new BasicDBObject("date", 1));

    // Insert
    collection.insert(new BasicDBObject("_id", 1).append("date", 1));

    IndexAbstract index = getIndex(collection, "date_1");
    assertEquals(1, index.size());

    collection.update(new BasicDBObject("_id", 1), new BasicDBObject("$unset", new BasicDBObject("date", 1)));
    assertEquals(0, index.size());
  }

  @Test
  public void indexesMustBeUsedForFindWithInFilter() {
    assumeFalse(fongoRule.isRealMongo());
    DBCollection collection = fongoRule.newCollection();

    collection.createIndex(new BasicDBObject("date", 1));

    for (int i = 0; i < 20; i++) {
      collection.insert(new BasicDBObject("date", i % 10).append("_id", i));
    }

    IndexAbstract indexDate = getIndex(collection, "date_1");

    assertEquals(0, indexDate.getLookupCount());

    List<DBObject> objects = collection.find(new BasicDBObject("date", new BasicDBObject("$in", Util.list(0, 1, 2)))).toArray();
    // No index used.
    assertEquals(1, indexDate.getLookupCount());
    assertEquals(6, objects.size());
  }

  @Test
  public void testFindOneOrData() {
    DBCollection collection = fongoRule.newCollection();
    collection.createIndex(new BasicDBObject("date", 1));
    collection.insert(new BasicDBObject("date", 1));
    DBObject result = collection.findOne(new BasicDBObject("$or", Util.list(new BasicDBObject("date", 1), new BasicDBObject("date", 2))));
    assertEquals(1, result.get("date"));
  }

  @Test
  public void testCompboudIndexFindOne() {
    DBCollection collection = fongoRule.newCollection();
    collection.createIndex(new BasicDBObject("date", -1).append("time", 1));
    collection.insert(new BasicDBObject("date", 1).append("time", 2));
    DBObject result = collection.findOne(new BasicDBObject("date", 1));
    assertEquals(1, result.get("date"));
    assertEquals(2, result.get("time"));
  }

  @Test
  public void should_mixed_index_works() {
    // Given
    DBCollection collection = fongoRule.newCollection();
    collection.createIndex(new BasicDBObject("object.sub", 1).append("second", 1));
    collection.insert(new BasicDBObject("_id", 1).append("object", new BasicDBObject("sub", 2).append("sib", 3)).append("second", 2));
    collection.insert(new BasicDBObject("_id", 2).append("object", new BasicDBObject("sub", 1).append("sib", 3)).append("second", 2));

    // When
    DBObject result = collection.findOne(new BasicDBObject("object.sub", 1));

    // Then
    Assertions.assertThat(result).isEqualTo(new BasicDBObject("_id", 2).append("object", new BasicDBObject("sub", 1).append("sib", 3)).append("second", 2));
  }

  @Test
  public void should_mixed_index_works_with_array() {
    // Given
    DBCollection collection = fongoRule.newCollection();
    collection.createIndex(new BasicDBObject("_id.sub", 1).append("second", 1));
//    collection.insert(new BasicDBObject("_id", new BasicDBObject("sub", 2)).append("second", Lists.newArrayList(10, 11, 12)));
    collection.insert(new BasicDBObject("_id", new BasicDBObject("sub", 1)).append("second", Lists.newArrayList(10, 11, 12)));

    // When
    DBObject result = collection.findOne(new BasicDBObject("_id.sub", 1).append("second", 10));

    // Then
    Assertions.assertThat(result).isEqualTo(new BasicDBObject("_id", new BasicDBObject("sub", 1)).append("second", Lists.newArrayList(10, 11, 12)));
  }

  @Test
  public void testIdInQueryResultsInIndexOnFieldOrder() {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("date", 4));
    collection.insert(new BasicDBObject("date", 3));
    collection.insert(new BasicDBObject("date", 1));
    collection.insert(new BasicDBObject("date", 2));
    collection.createIndex(new BasicDBObject("date", 1));

    DBCursor cursor = collection.find(new BasicDBObject("date",
        new BasicDBObject("$in", Arrays.asList(3, 2, 1))), new BasicDBObject("date", 1).append("_id", 0));
    assertEquals(Arrays.asList(
        new BasicDBObject("date", 1),
        new BasicDBObject("date", 2),
        new BasicDBObject("date", 3)
    ), cursor.toArray());
  }

  @Test
  public void test2dIndex() {
    assumeFalse(fongoRule.isRealMongo());
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("loc", Util.list(-73.97D, 40.72D)));
    collection.insert(new BasicDBObject("_id", 2).append("loc", Util.list(2.265D, 48.791D)));
    collection.createIndex(new BasicDBObject("loc", "2d"));

    IndexAbstract index = getIndex(collection, "loc_2d");
    assertTrue(index.isGeoIndex());
  }

  @Test(expected = MongoCommandException.class)
  public void test2dIndexNotFirst() {
    DBCollection collection = fongoRule.newCollection();
// com.mongodb.CommandFailureException: { "serverUsed" : "127.0.0.1:27017" , "createdCollectionAutomatically" : false , "numIndexesBefore" : 1 , "errmsg" : "exception: 2d has to be first in index" , "code" : 16801 , "ok" : 0.0}

    collection.insert(new BasicDBObject("_id", 1).append("loc", Util.list(-73.97D, 40.72D)));
    collection.insert(new BasicDBObject("_id", 2).append("loc", Util.list(2.265D, 48.791D)));
    collection.createIndex(new BasicDBObject("name", 1).append("loc", "2d"));
  }

  @Test
  public void test2dsphereIndex() {
    assumeFalse(fongoRule.isRealMongo());
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("loc", Util.list(-73.97D, 40.72D)));
    collection.insert(new BasicDBObject("_id", 2).append("loc", Util.list(2.265D, 48.791D)));
    collection.createIndex(new BasicDBObject("loc", "2dsphere"));

    IndexAbstract<?> index = getIndex(collection, "loc_2dsphere");
    assertTrue(index.isGeoIndex());
  }

  /**
   * 2dsphere indexes are not required to be first in compound indexes.
   *
   * @see <a href="https://github.com/fakemongo/fongo/issues/65">Issue 65</a>
   */
  @Test
  public void test2dsphereNotFirst() {
    assumeFalse(fongoRule.isRealMongo());
    DBCollection collection = fongoRule.newCollection();

    collection.insert(new BasicDBObject("_id", 1).append("name", "a").append("loc", Util.list(-73.97D, 40.72D)));
    collection.insert(new BasicDBObject("_id", 2).append("name", "b").append("loc", Util.list(2.265D, 48.791D)));
    collection.createIndex(new BasicDBObject("name", 1).append("loc", "2dsphere"));

    IndexAbstract<?> index = getIndex(collection, "name_1_loc_2dsphere");
    assertTrue(index.isGeoIndex());
  }

  @Test
  public void testUpdateMustModifyAllIndexes() throws Exception {
    assumeFalse(fongoRule.isRealMongo());
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("date", 1).append("name", "jon").append("_id", 1));
    collection.createIndex(new BasicDBObject("date", 1));
    collection.createIndex(new BasicDBObject("name", 1));

    IndexAbstract indexDate = getIndex(collection, "date_1");
    IndexAbstract indexName = getIndex(collection, "name_1");

    // Now, modify an object.
    collection.update(new BasicDBObject("_id", 1), new BasicDBObject("$set", new BasicDBObject("name", "will")));
    assertEquals(0, indexDate.getLookupCount());
    assertEquals(0, indexName.getLookupCount());

    // Find in with index "name"
    assertEquals(new BasicDBObject("date", 1).append("name", "will").append("_id", 1), collection.findOne(new BasicDBObject("name", "will")));
    assertEquals(1, indexName.getLookupCount());
    assertEquals(0, indexDate.getLookupCount());
    assertEquals(new BasicDBObject("date", 1).append("name", "will").append("_id", 1), collection.findOne(new BasicDBObject("date", 1)));
    assertEquals(1, indexName.getLookupCount());
    assertEquals(1, indexDate.getLookupCount());
    assertEquals(new BasicDBObject("date", 1).append("name", "will").append("_id", 1), collection.findOne(new BasicDBObject("_id", 1)));
    assertEquals(1, indexDate.getLookupCount());
    assertEquals(1, indexName.getLookupCount());
  }

  @Test
  @Ignore("strange index, Mongo doesn't handle but no exception.")
  public void testInnerIndex() throws Exception {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("a", new BasicDBObject("n", 1)));

    assertEquals(
        new BasicDBObject("_id", 1).append("a", new BasicDBObject("n", 1)),
        collection.findOne(new BasicDBObject("a.n", 1))
    );

    collection.createIndex(new BasicDBObject("a.n", 1));
    IndexAbstract index = getIndex(collection, "a.n_1");
    assertEquals(
        new BasicDBObject("_id", 1).append("a", new BasicDBObject("n", 1)),
        collection.findOne(new BasicDBObject("a.n", 1))
    );
    assertEquals(1, index.getLookupCount());
  }

  @Test
  public void testStrangeIndexThrowException() throws Exception {
    ExpectedMongoException.expectCode(exception, 67, MongoCommandException.class);
    DBCollection collection = fongoRule.newCollection();
    collection.createIndex(new BasicDBObject("a", new BasicDBObject("n", 1)));
  }

  // Creating an index after inserting into a collection must add records only if necessary
  @Test
  public void testCreateIndexLater() throws Exception {
    assumeFalse(fongoRule.isRealMongo());
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("a", 1));
    collection.insert(new BasicDBObject("_id", 2));
    collection.createIndex(new BasicDBObject("a", 1));

    IndexAbstract index = getIndex(collection, "a_1");
    assertEquals(1, index.size());
  }

  // Creating an index before inserting into a collection must add records only if necessary
  @Test
  public void testCreateIndexBefore() throws Exception {
    assumeFalse(fongoRule.isRealMongo());
    DBCollection collection = fongoRule.newCollection();
    collection.createIndex(new BasicDBObject("a", 1));
    collection.insert(new BasicDBObject("_id", 1).append("a", 1));
    collection.insert(new BasicDBObject("_id", 2));

    IndexAbstract index = getIndex(collection, "a_1");
    assertEquals(1, index.size());
  }

  @Test
  public void testRemoveMulti() throws Exception {
    assumeFalse(fongoRule.isRealMongo());
    DBCollection collection = fongoRule.newCollection();
    collection.createIndex(new BasicDBObject("a", 1));
    collection.insert(new BasicDBObject("_id", 1).append("a", 1));
    collection.insert(new BasicDBObject("_id", 2));
    collection.insert(new BasicDBObject("_id", 3).append("a", 1));

    IndexAbstract index = getIndex(collection, "a_1");
    assertEquals(2, index.size());

    collection.remove(new BasicDBObject("a", 1));
    assertEquals(0, index.size());
  }

  @Test
  public void should_handled_hashed_index() throws Exception {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("date", 4));
    collection.insert(new BasicDBObject("date", 3));
    collection.insert(new BasicDBObject("date", 1));
    collection.insert(new BasicDBObject("date", 2));
    collection.insert(new BasicDBObject("date", 2));
    collection.insert(new BasicDBObject("things", 6));
    collection.createIndex(new BasicDBObject("date", "hashed"));

    DBCursor cursor = collection.find(new BasicDBObject("date",
        new BasicDBObject("$in", Arrays.asList(3, 2, 1))), new BasicDBObject("date", 1).append("_id", 0)).sort(new BasicDBObject("date", 1));
    assertEquals(Arrays.asList(
        new BasicDBObject("date", 1),
        new BasicDBObject("date", 2),
        new BasicDBObject("date", 2),
        new BasicDBObject("date", 3)
    ), cursor.toArray());
  }

  @Test
  public void should_handled_hashed_index_on__id() throws Exception {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("_id", 4));
    collection.insert(new BasicDBObject("_id", 3));
    collection.insert(new BasicDBObject("_id", 1));
    collection.insert(new BasicDBObject("_id", 2));
    collection.createIndex(new BasicDBObject("_id", "hashed"));

    DBCursor cursor = collection.find(new BasicDBObject("_id",
        new BasicDBObject("$in", Arrays.asList(3, 2, 1)))).sort(new BasicDBObject("_id", 1));
    assertEquals(Arrays.asList(
        new BasicDBObject("_id", 1),
        new BasicDBObject("_id", 2),
        new BasicDBObject("_id", 3)
    ), cursor.toArray());
  }

  @Test
  public void should_not_handled_hashed_index_on_array_before() throws Exception {
    ExpectedMongoException.expectCode(exception, 16766, MongoCommandException.class);
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("date", new BasicDBList()));
    collection.createIndex(new BasicDBObject("date", "hashed"));
  }

  @Test
  public void should_not_handled_hashed_index_on_array_after() throws Exception {
    ExpectedMongoException.expectCode(exception, 16766, MongoException.class);
    DBCollection collection = fongoRule.newCollection();
    collection.createIndex(new BasicDBObject("date", "hashed"));
    collection.insert(new BasicDBObject("date", new BasicDBList()));
  }

  @Test
  public void should_not_dropIndex_interfer_between_collection() {
    // Given
    DBCollection collection1 = fongoRule.newCollection();
    DBCollection collection2 = fongoRule.newCollection();
    collection1.createIndex(new BasicDBObject("date", 1));
    collection2.createIndex(new BasicDBObject("date", 1));

    // When
    collection1.dropIndex(new BasicDBObject("date", 1));

    // Then
    Assertions.assertThat(collection1.getIndexInfo()).hasSize(1);
    Assertions.assertThat(collection2.getIndexInfo()).hasSize(2);
  }

  @Test
  public void should_not_dropIndexes_interfer_between_collection() {
    // Given
    DBCollection collection1 = fongoRule.newCollection();
    DBCollection collection2 = fongoRule.newCollection();
    collection1.createIndex(new BasicDBObject("date", 1));
    collection2.createIndex(new BasicDBObject("date", 1));

    // When
    collection1.dropIndexes();

    // Then
    Assertions.assertThat(collection1.getIndexInfo()).hasSize(1);
    Assertions.assertThat(collection2.getIndexInfo()).hasSize(2);
  }

  @Test
  public void should_not_drop_interfer_between_collection() {
    // Given
    DBCollection collection1 = fongoRule.newCollection();
    DBCollection collection2 = fongoRule.newCollection();
    collection1.createIndex(new BasicDBObject("date", 1));
    collection2.createIndex(new BasicDBObject("date", 1));

    // When
    collection1.drop();

    // Then
    Assertions.assertThat(collection2.getIndexInfo()).hasSize(2);
    // Assertions.assertThat(collection1.getIndexInfo()).hasSize(0);
  }
}