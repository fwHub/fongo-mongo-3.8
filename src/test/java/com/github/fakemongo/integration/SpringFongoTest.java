package com.github.fakemongo.integration;

import com.github.fakemongo.Fongo;
import com.mongodb.MongoClient;
import org.assertj.core.api.Assertions;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexed;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.hateoas.Identifiable;

import java.io.Serializable;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.springframework.data.mongodb.core.query.Criteria.where;

public class SpringFongoTest {

  @Test
  public void dBRefFindWorks() {
    ApplicationContext ctx = new AnnotationConfigApplicationContext(MongoConfig.class);
    MongoOperations mongoOperations = (MongoOperations) ctx.getBean("mongoTemplate");

    MainObject mainObject = new MainObject();

    ReferencedObject referencedObject = new ReferencedObject();

    mainObject.setReferencedObject(referencedObject);

    mongoOperations.save(referencedObject);
    mongoOperations.save(mainObject);

    MainObject foundObject = mongoOperations.findOne(
        new Query(where("referencedObject.$id").is(new ObjectId(referencedObject.getId()))),
        MainObject.class);

    assertNotNull("should have found an object", foundObject);
    assertEquals("should find a ref to an object", referencedObject.getId(), foundObject.getReferencedObject().getId());
  }

  @Test
  public void should_listdBRefFindWorks() {
    ApplicationContext ctx = new AnnotationConfigApplicationContext(MongoConfig.class);
    MongoOperations mongoOperations = (MongoOperations) ctx.getBean("mongoTemplate");

    MainListObject mainObject = new MainListObject();

    List<ReferencedObject> referencedObjects = new ArrayList<ReferencedObject>();
    for (int i = 0; i < 3; i++) {
      final ReferencedObject referencedObject = new ReferencedObject();
      referencedObjects.add(referencedObject);
      mongoOperations.save(referencedObject);
    }

    mainObject.setReferencedObjects(referencedObjects);

    mongoOperations.save(mainObject);

    MainListObject foundObject = mongoOperations.findOne(
        new Query(where("referencedObjects.$id").is(new ObjectId(referencedObjects.get(0).getId()))),
        MainListObject.class);

    assertNotNull("should have found an object", foundObject);
    Assertions.assertThat(foundObject.getReferencedObjects()).containsExactlyElementsOf(referencedObjects);
  }

  @Ignore
  @Test
  public void testGeospacialIndexed() {
    // Given
    ApplicationContext ctx = new AnnotationConfigApplicationContext(MongoConfig.class);
    MongoOperations mongoOperations = (MongoOperations) ctx.getBean("mongoTemplate");

    GeoSpatialIndexedWrapper object = new GeoSpatialIndexedWrapper();
    object.setGeo(new double[]{12.335D, 13.546D});

    // When
    mongoOperations.save(object);

    // Then
    assertEquals(object, mongoOperations.findOne(
        new Query(where("id").is(new ObjectId(object.getId()))),
        GeoSpatialIndexedWrapper.class));
    assertEquals(object, mongoOperations.findOne(
        new Query(where("geo").is(object.getGeo())),
        GeoSpatialIndexedWrapper.class));
    assertEquals(object, mongoOperations.findOne(
        new Query(where("geo").is(new Point(object.getGeo()[0], object.getGeo()[1]))),
        GeoSpatialIndexedWrapper.class));
  }

  @Test
  public void testGeoNear() {
    // Given
    ApplicationContext ctx = new AnnotationConfigApplicationContext(MongoConfig.class);
    MongoOperations mongoOperations = (MongoOperations) ctx.getBean("mongoTemplate");

    GeoSpatialIndexedWrapper object = new GeoSpatialIndexedWrapper();
    object.setGeo(new double[]{12.335D, 13.546D});

    // When
    mongoOperations.save(object);

    // Then
    final GeoResults<GeoSpatialIndexedWrapper> geoResults = mongoOperations.geoNear(NearQuery.near(13.54, 12.33, Metrics.KILOMETERS), GeoSpatialIndexedWrapper.class);
    assertThat(geoResults).hasSize(1);
    assertThat(geoResults.getContent().get(0).getContent()).isEqualTo(object);
  }

  @Test
  public void testMongoRepository() {
    // Given
    ApplicationContext ctx = new AnnotationConfigApplicationContext(MongoConfig.class);
    TestRepository mongoRepository = ctx.getBean(TestRepository.class);

    ReferencedObject referencedObject = new ReferencedObject();

    // When
    mongoRepository.save(referencedObject);

    // Then
    Assert.assertEquals(Optional.of(referencedObject), mongoRepository.findById(referencedObject.getId()));
  }

  @Test
  public void testMongoRepositoryFindAll() {
    // Given
    ApplicationContext ctx = new AnnotationConfigApplicationContext(MongoConfig.class);
    TestRepository mongoRepository = ctx.getBean(TestRepository.class);
    mongoRepository.save(new ReferencedObject("a"));
    mongoRepository.save(new ReferencedObject("b"));
    mongoRepository.save(new ReferencedObject("c"));
    mongoRepository.save(new ReferencedObject("d"));

    // When
    Page<ReferencedObject> result = mongoRepository.findAll(new PageRequest(0, 10, Sort.Direction.DESC, "_id"));

    // Then
    assertEquals(Arrays.asList(
        new ReferencedObject("d"),
        new ReferencedObject("c"),
        new ReferencedObject("b"),
        new ReferencedObject("a")), result.getContent());
  }

  @Test
  public void testMongoRepositoryFindAllSortId() {
    // Given
    ApplicationContext ctx = new AnnotationConfigApplicationContext(MongoConfig.class);
    TestRepository mongoRepository = ctx.getBean(TestRepository.class);
    mongoRepository.save(new ReferencedObject("d"));
    mongoRepository.save(new ReferencedObject("c"));
    mongoRepository.save(new ReferencedObject("b"));
    mongoRepository.save(new ReferencedObject("a"));

    // When
    List<ReferencedObject> result = new ArrayList<ReferencedObject>(mongoRepository.findAll(new Sort("_id")));

    // Then
    assertEquals(Arrays.asList(
        new ReferencedObject("a"),
        new ReferencedObject("b"),
        new ReferencedObject("c"),
        new ReferencedObject("d")), result);
  }

  @Ignore
  @Test
  public void testMapLookup() throws Exception {
    ApplicationContext ctx = new AnnotationConfigApplicationContext(MongoConfig.class);
    SpringModelMapRepository springModelMapRepository = ctx.getBean(SpringModelMapRepository.class);
    String serial = "serial1";
    Date time = new Date();
    Map<String, Object> springModelBarcode = new HashMap<String, Object>();
    springModelBarcode.put("serial", serial);
    springModelBarcode.put("time", time);
    SpringModelMap map = new SpringModelMap(springModelBarcode);
    springModelMapRepository.save(map);
    MongoOperations mongoOperations = (MongoOperations) ctx.getBean("mongoTemplate");
    Query query = Query.query(where("barcode.serial").is(serial).and("barcode.time").is(time));
    List<SpringModelMap> receipts = mongoOperations.find(query, SpringModelMap.class);
    assertEquals(1, receipts.size());
  }

  @Configuration
  @EnableMongoRepositories
  public static class MongoConfig extends AbstractMongoConfiguration {

    @Override
    protected String getDatabaseName() {
      return "db";
    }

    @Override
    @Bean
    public MongoClient mongoClient() {
//      return new MongoClient();
      return new Fongo("spring-test").getMongo();
    }

    @Bean
    protected String getMappingBasePackage() {
      return TestRepository.class.getPackage().getName();
    }
  }

  @Document
  public static class ReferencedObject implements Serializable, Identifiable<String> {

    private static final long serialVersionUID = 1L;
    @Id
    private String id;

    public ReferencedObject() {
    }

    public ReferencedObject(String id) {
      this.id = id;
    }

    @Override
    public String getId() {
      return this.id;
    }

    @Override
    public String toString() {
      return "ReferencedObject{" +
          "id='" + id + '\'' +
          '}';
    }

    public void setId(String id) {
      this.id = id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof ReferencedObject)) return false;

      ReferencedObject that = (ReferencedObject) o;

      return !(id != null ? !id.equals(that.id) : that.id != null);

    }

    @Override
    public int hashCode() {
      return id != null ? id.hashCode() : 0;
    }
  }

  @Document
  public static class MainObject implements Serializable, Identifiable<String> {

    private static final long serialVersionUID = 1L;

    @Id
    private String id;

    @DBRef
    private ReferencedObject referencedObject;

    @Override
    public String getId() {
      return this.id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public ReferencedObject getReferencedObject() {
      return referencedObject;
    }

    public void setReferencedObject(ReferencedObject referencedObject) {
      this.referencedObject = referencedObject;
    }
  }

  @Document
  public static class MainListObject implements Serializable, Identifiable<String> {

    private static final long serialVersionUID = 1L;

    @Id
    private String id;

    @DBRef
    private List<ReferencedObject> referencedObjects;

    @Override
    public String getId() {
      return this.id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public List<ReferencedObject> getReferencedObjects() {
      return referencedObjects;
    }

    public void setReferencedObjects(List<ReferencedObject> referencedObjects) {
      this.referencedObjects = referencedObjects;
    }
  }

  @Document
  public class GeoSpatialIndexedWrapper {
    @Id
    private String id;

    @GeoSpatialIndexed
    private double[] geo = new double[]{0D, 0D};

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public double[] getGeo() {
      return geo;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof GeoSpatialIndexedWrapper)) return false;

      GeoSpatialIndexedWrapper that = (GeoSpatialIndexedWrapper) o;

      return id.equals(that.id) && Arrays.equals(geo, that.geo);

    }

    @Override
    public int hashCode() {
      int result = id.hashCode();
      result = 31 * result + Arrays.hashCode(geo);
      return result;
    }

    public void setGeo(double[] geo) {
      this.geo = geo;
    }

  }
}
