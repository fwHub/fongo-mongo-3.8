package com.github.fakemongo;

import com.github.fakemongo.impl.ExpressionParser;
import com.github.fakemongo.impl.geo.GeoUtil;
import com.github.fakemongo.impl.index.IndexAbstract;
import com.mongodb.*;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.bulk.UpdateRequest;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.bulk.WriteRequest;
import com.mongodb.bulk.*;
import com.mongodb.connection.*;
import com.mongodb.internal.connection.IndexMap;
import com.mongodb.internal.validator.CollectibleDocumentFieldNameValidator;
import com.mongodb.internal.validator.UpdateFieldNameValidator;
import com.mongodb.operation.FongoBsonArrayWrapper;
import com.mongodb.session.SessionContext;
import com.mongodb.util.JSON;
import com.vividsolutions.jts.geom.Coordinate;
import org.bson.*;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.mongodb.FongoDBCollection.*;
import static com.mongodb.bulk.WriteRequest.Type.*;
import static java.util.Arrays.asList;

/**
 *
 */
public class FongoConnection implements Connection {
  private final static Logger LOG = LoggerFactory.getLogger(FongoConnection.class);

  private final Fongo fongo;
  private final ConnectionDescription connectionDescription;

  public FongoConnection(final Fongo fongo) {
    this.fongo = fongo;
    this.connectionDescription = new ConnectionDescription(new ServerId(new ClusterId(), fongo.getServerAddress())) {
      @Override
      public ServerVersion getServerVersion() {
        return fongo.getServerVersion();
      }
    };
  }

  @Override
  public Connection retain() {
    LOG.debug("retain()");
    return this;
  }

  @Override
  public ConnectionDescription getDescription() {
    return connectionDescription;
  }

  @Override
  public WriteConcernResult insert(MongoNamespace namespace, boolean ordered, InsertRequest insert) {
    LOG.debug("insert() namespace:{} insert:{}", namespace, insert);
    final DBCollection collection = dbCollection(namespace);
      final DBObject parse = dbObject(insert.getDocument());
      collection.insert(parse);
      LOG.debug("insert() namespace:{} insert:{}, parse:{}", namespace, insert.getDocument(), parse.getClass());
      return WriteConcernResult.acknowledged(1, false, null);
  }

  @Override
  public WriteConcernResult update(MongoNamespace namespace, boolean ordered, UpdateRequest update) {
    LOG.debug("update() namespace:{} update:{}", namespace, update);
    final DBCollection collection = dbCollection(namespace);

    boolean isUpdateOfExisting = false;
    BsonValue upsertedId = null;
    int count = 0;

    FieldNameValidator validator;
    if (update.getType() == REPLACE) {
      validator = new CollectibleDocumentFieldNameValidator();
    } else {
      validator = new UpdateFieldNameValidator();
    }
    for (String updateName : update.getUpdate().keySet()) {
      if (!validator.validate(updateName)) {
        throw new IllegalArgumentException("Invalid BSON field name " + updateName);
      }
    }
    final WriteResult writeResult = collection.update(dbObject(update.getFilter()), dbObject(update.getUpdate()), update.isUpsert(), update.isMulti());
    if (writeResult.isUpdateOfExisting()) {
      isUpdateOfExisting = true;
      count += writeResult.getN();
    } else {
      if (update.isUpsert()) {
        BsonValue updateId = update.getUpdate().get(DBCollection.ID_FIELD_NAME, null);

        if (updateId != null) {
          upsertedId = updateId;
        } else {
          BsonDocument bsonDoc = bsonDocument(new BasicDBObject(DBCollection.ID_FIELD_NAME, writeResult.getUpsertedId()));
          upsertedId = bsonDoc.get(DBCollection.ID_FIELD_NAME);
        }
        count++;
      } else {
        count += writeResult.getN();
      }
    }
    return WriteConcernResult.acknowledged(count, isUpdateOfExisting, upsertedId);
  }

  @Override
  public WriteConcernResult delete(MongoNamespace namespace, boolean ordered, DeleteRequest delete) {
    LOG.debug("delete() namespace:{} delete:{}", namespace, delete);
    final DBCollection collection = dbCollection(namespace);
    int count = delete(collection, delete);
    return WriteConcernResult.acknowledged(count, count != 0, null);
  }

  public BulkWriteResult insertCommand(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<InsertRequest> inserts) {
    return this.insertCommand(namespace, ordered, writeConcern, false, inserts);
  }

  /**
   * Insert the documents using the insert command.
   *
   * @param namespace                the namespace
   * @param ordered                  whether the writes are ordered
   * @param writeConcern             the write concern
   * @param bypassDocumentValidation the bypassDocumentValidation flag
   * @param inserts                  the inserts
   * @return the bulk write result
   * @since 3.2
   */
  public BulkWriteResult insertCommand(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, Boolean bypassDocumentValidation, List<InsertRequest> inserts) {
    LOG.debug("insertCommand() namespace:{} inserts:{}", namespace, inserts);
    final DBCollection collection = dbCollection(namespace);
    validateCollectionName(collection.getName());
    BulkWriteBatchCombiner bulkWriteBatchCombiner = new BulkWriteBatchCombiner(fongo.getServerAddress(), ordered, writeConcern);
    IndexMap indexMap = IndexMap.create();
    final BulkWriteOperation bulkWriteOperation = ordered ? collection.initializeOrderedBulkOperation() : collection.initializeUnorderedBulkOperation();

    try {
      for (InsertRequest insert : inserts) {
        if (!Boolean.TRUE.equals(bypassDocumentValidation)) {
          FieldNameValidator validator = new CollectibleDocumentFieldNameValidator();

          for (String updateName : insert.getDocument().keySet()) {
            if (!validator.validate(updateName)) {
              throw new IllegalArgumentException("Invalid BSON field name " + updateName);
            }
          }
        }

        bulkWriteOperation.insert(dbObject(insert.getDocument()));
        indexMap = indexMap.add(1, 0);
      }
      final com.mongodb.BulkWriteResult bulkWriteResult = bulkWriteOperation.execute(writeConcern);
      bulkWriteBatchCombiner.addResult(bulkWriteResult(bulkWriteResult), indexMap);
    } catch (InsertManyWriteConcernException writeException) {
      bulkWriteBatchCombiner.addResult(bulkWriteResult(writeException.getResult()), indexMap);
      for (FongoBulkWriteCombiner.WriteError writeError : writeException.getErrors()) {
        indexMap.add(writeError.getIndex(), writeError.getIndex());
        bulkWriteBatchCombiner.addWriteErrorResult(getBulkWriteError(writeError.getException(), writeError.getIndex()), indexMap);
      }
    } catch (BulkWriteException bulkWriteException) {
      // we need to pull the information out of the BulkWriteException and package it up in bulkWriteBatchCombiner
      // bulkWriteBatchCombiner.getResult() will end up throwing a MongoBulkWriteException (NOT a BulkWriteException!)
      // because that's what mongo does??
      BulkWriteResult transformed = FongoDBCollection.translateBulkWriteResultToNew(bulkWriteException.getWriteResult());
      bulkWriteBatchCombiner.addResult(transformed, indexMap);
      for (com.mongodb.bulk.BulkWriteError writeError : FongoDBCollection.translateWriteErrorsToNew(bulkWriteException.getWriteErrors())) {
        indexMap.add(writeError.getIndex(), writeError.getIndex());
        bulkWriteBatchCombiner.addWriteErrorResult(writeError, indexMap);
      }
    } catch (WriteConcernException writeException) {
      if (writeException.getResponse().get("wtimeout") != null) {
        bulkWriteBatchCombiner.addWriteConcernErrorResult(getWriteConcernError(writeException));
      } else {
        bulkWriteBatchCombiner.addWriteErrorResult(getBulkWriteError(writeException), indexMap);
      }
    }
    return bulkWriteBatchCombiner.getResult();
  }

  private void validateCollectionName(String collectionName) {
    if (collectionName == null || collectionName.isEmpty() || collectionName.startsWith("system.")
            || collectionName.contains("$") || collectionName.contains("\0"))
      throw new IllegalArgumentException("Invalid collection name " + collectionName);
  }

  private static final List<String> IGNORED_KEYS = asList("ok", "err", "code");

  BulkWriteError getBulkWriteError(final WriteConcernException writeException) {
    return getBulkWriteError(writeException, 0);
  }

  BulkWriteError getBulkWriteError(final WriteConcernException writeException, int index) {
    return new BulkWriteError(writeException.getErrorCode(), writeException.getErrorMessage(),
        translateGetLastErrorResponseToErrInfo(writeException.getResponse()), index);
  }

  WriteConcernError getWriteConcernError(final WriteConcernException writeException) {
    return new WriteConcernError(writeException.getErrorCode(),
        ((BsonString) writeException.getResponse().get("err")).getValue(),
        translateGetLastErrorResponseToErrInfo(writeException.getResponse()));
  }

  private BsonDocument translateGetLastErrorResponseToErrInfo(final BsonDocument response) {
    BsonDocument errInfo = new BsonDocument();
    for (Map.Entry<String, BsonValue> entry : response.entrySet()) {
      if (IGNORED_KEYS.contains(entry.getKey())) {
        continue;
      }
      errInfo.put(entry.getKey(), entry.getValue());
    }
    return errInfo;
  }

  /**
   * Update the documents using the update command.
   *
   * @param namespace    the namespace
   * @param ordered      whether the writes are ordered
   * @param writeConcern the write concern
   * @param updates      the updates
   * @return the bulk write result
   * @since 3.2
   */
  public BulkWriteResult updateCommand(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<UpdateRequest> updates) {
    return this.updateCommand(namespace, ordered, writeConcern, false, updates);
  }

  /**
   * Update the documents using the update command.
   *
   * @param namespace                the namespace
   * @param ordered                  whether the writes are ordered
   * @param writeConcern             the write concern
   * @param bypassDocumentValidation the bypassDocumentValidation flag
   * @param updates                  the updates
   * @return the bulk write result
   * @since 3.2
   */
  public BulkWriteResult updateCommand(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, Boolean bypassDocumentValidation, List<UpdateRequest> updates) {
    LOG.debug("updateCommand() namespace:{} updates:{}", namespace, updates);
    final FongoDBCollection collection = dbCollection(namespace);

    BulkWriteBatchCombiner bulkWriteBatchCombiner = new BulkWriteBatchCombiner(fongo.getServerAddress(), ordered, writeConcern);

    int idx = 0, offset = 0;
    for (UpdateRequest update : updates) {
      IndexMap indexMap = IndexMap.create(offset, 1);
      final BulkWriteOperation bulkWriteOperation = collection.initializeOrderedBulkOperation();

      if (Boolean.TRUE.equals(bypassDocumentValidation)) {
        FieldNameValidator validator;
        if (update.getType() == REPLACE || update.getType() == INSERT) {
          validator = new CollectibleDocumentFieldNameValidator();
        } else {
          validator = new UpdateFieldNameValidator();
        }
        for (String updateName : update.getUpdate().keySet()) {
          if (!validator.validate(updateName)) {
            throw new IllegalArgumentException("Invalid BSON field name " + updateName);
          }
        }
      }

      switch (update.getType()) {
        case REPLACE:
          if (update.isUpsert()) {
            bulkWriteOperation.find(dbObject(update.getFilter())).upsert().replaceOne(dbObject(update.getUpdate()));
          } else {
            bulkWriteOperation.find(dbObject(update.getFilter())).replaceOne(dbObject(update.getUpdate()));
          }
          break;
        case INSERT:
          bulkWriteOperation.insert(dbObject(update.getUpdate()));
          break;
        case UPDATE: {
          if (update.isUpsert()) {
            final BulkUpdateRequestBuilder upsert = bulkWriteOperation.find(dbObject((update.getFilter()))).upsert();
            if (update.isMulti()) {
              upsert.update(dbObject(update.getUpdate()));
            } else {
              upsert.updateOne(dbObject(update.getUpdate()));
            }
          } else {
            BulkWriteRequestBuilder bulkWriteRequestBuilder = bulkWriteOperation.find(dbObject((update.getFilter())));
            if (update.isMulti()) {
              bulkWriteRequestBuilder.update(dbObject(update.getUpdate()));
            } else {
              bulkWriteRequestBuilder.updateOne(dbObject(update.getUpdate()));
            }
          }
        }
        break;
        case DELETE:
          bulkWriteOperation.find(dbObject((update.getFilter()))).removeOne();
      }

//      collection.executeBulkWriteOperation()
      final com.mongodb.BulkWriteResult bulkWriteResult = bulkWriteOperation.execute(writeConcern);
      indexMap = indexMap.add(0, offset);
      BulkWriteResult bwr = bulkWriteResult(bulkWriteResult);
      int upsertCount = bwr.getUpserts().size();
      offset += Math.max(upsertCount, 1);
      bulkWriteBatchCombiner.addResult(bwr, indexMap);
      idx++;
    }
    return bulkWriteBatchCombiner.getResult();
  }

  public BulkWriteResult deleteCommand(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<DeleteRequest> deletes) {
    LOG.debug("deleteCommand() namespace:{} deletes:{}", namespace, deletes);
    final DBCollection collection = dbCollection(namespace);
    int count = delete(collection, writeConcern, deletes);
    if (writeConcern.isAcknowledged()) {
      return BulkWriteResult.acknowledged(WriteRequest.Type.DELETE, count, writeConcern.isAcknowledged() ? deletes.size() : null, Collections.<BulkWriteUpsert>emptyList());
    } else {
      return BulkWriteResult.unacknowledged();
    }
  }

  private int delete(DBCollection collection, DeleteRequest delete) {
    int count = 0;
    final DBObject parse = dbObject(delete.getFilter());
    if (delete.isMulti()) {
      final WriteResult writeResult = collection.remove(parse);
      count += writeResult.getN();
    } else {
      final DBObject dbObject = collection.findAndRemove(parse);
      if (dbObject != null) {
        count++;
      }
    }
    return count;
  }

  private int delete(DBCollection collection, WriteConcern writeConcern, List<DeleteRequest> deletes) {
    int count = 0;
    for (DeleteRequest delete : deletes) {
      final DBObject parse = dbObject(delete.getFilter());
      if (delete.isMulti()) {
        final WriteResult writeResult = collection.remove(parse, writeConcern);
        count += writeResult.getN();
      } else {
        final DBObject dbObject = collection.findAndRemove(parse);
        if (dbObject != null) {
          count++;
        }
      }
    }
    return count;
  }

  @Override
  public <T> T command(String database, BsonDocument command, FieldNameValidator fieldNameValidator, ReadPreference readPreference, Decoder<T> commandResultDecoder, SessionContext sessionContext) {
    return command(database, command, true, fieldNameValidator, commandResultDecoder);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T command(String database, BsonDocument command, FieldNameValidator fieldNameValidator, ReadPreference readPreference, Decoder<T> commandResultDecoder, SessionContext sessionContext, boolean responseExpected, SplittablePayload payload, FieldNameValidator payloadFieldNameValidator) {
    final DB db = fongo.getDB(database);

    if (command.containsKey("delete")) {
      return (T) executeDeleteCommand(db, getString(command, "delete"), command, payload);
    } else if (command.containsKey("insert")) {
      return (T) executeInsertCommand(db, getString(command, "insert"), command, payload);
    } else if (command.containsKey("update")) {
      return (T) executeUpdateCommand(db, getString(command, "update"), command, payload);
    }

    System.err.println(command + "\n\t" + payload.getPayload());
    throw new FongoException("Not implemented for command : " + JSON.serialize(dbObject(command)));
  }

  private BsonDocument executeDeleteCommand(DB db, String collectionName, BsonDocument command, SplittablePayload payload) {
    List<BsonDocument> docs = consumePayload(payload);

    List<DeleteRequest> deleteRequests = new ArrayList<DeleteRequest>(docs.size());
    for (BsonDocument doc : docs) {
      deleteRequests.add(new DeleteRequest(doc.getDocument("q"))
          .multi(doc.getInt32("limit", new BsonInt32(0)).intValue() == 0));
    }

    boolean ordered = getBooleanOrFalse(command, "ordered");

    MongoNamespace ns = new MongoNamespace(db.getName(), collectionName);

    BulkWriteResult result = deleteCommand(ns, ordered, getWriteConcern(db, collectionName), deleteRequests);

    return new BsonDocument("ok", BsonBoolean.TRUE).append("n", new BsonInt32(result.getDeletedCount()));
  }

  private BsonDocument executeInsertCommand(DB db, String collectionName, BsonDocument command, SplittablePayload payload) {
    List<BsonDocument> docs = consumePayload(payload);

    List<InsertRequest> insertRequests = new ArrayList<InsertRequest>(docs.size());
    for (BsonDocument doc : docs) {
      insertRequests.add(new InsertRequest(doc));
    }

    boolean ordered = getBooleanOrFalse(command, "ordered");

    MongoNamespace ns = new MongoNamespace(db.getName(), collectionName);

    BulkWriteResult result = insertCommand(ns, ordered, getWriteConcern(db, collectionName), null, insertRequests);

    return new BsonDocument("ok", BsonBoolean.TRUE).append("n", new BsonInt32(result.getInsertedCount()));
  }

  private BsonDocument executeUpdateCommand(DB db, String collectionName, BsonDocument command, SplittablePayload payload) {
    List<BsonDocument> docs = consumePayload(payload);

    List<UpdateRequest> updateRequests = new ArrayList<UpdateRequest>(docs.size());
    for (BsonDocument doc : docs) {
      BsonDocument update = doc.getDocument("u");
      updateRequests.add(new UpdateRequest(doc.getDocument("q"), update, convertPayloadType(payload.getPayloadType()))
          .upsert(getBooleanOrFalse(doc, "upsert")));
    }

    boolean ordered = getBooleanOrFalse(command, "ordered");

    MongoNamespace ns = new MongoNamespace(db.getName(), collectionName);

    BulkWriteResult result = updateCommand(ns, ordered, getWriteConcern(db, collectionName), null, updateRequests);

    int matchCount = result.getMatchedCount();
    int modifiedCount = result.getModifiedCount();
    List<BulkWriteUpsert> upserts = result.getUpserts();
    int upsertsCount = upserts.size();

    BsonArray upserted = new BsonArray();
    for (BulkWriteUpsert upsert : upserts) {
      upserted.add(new BsonDocument("index", new BsonInt32(upsert.getIndex())).append("_id", upsert.getId()));
    }

    return new BsonDocument("ok", BsonBoolean.TRUE)
        .append("n", new BsonInt32(matchCount + upsertsCount))
        .append("nModified", new BsonInt32(modifiedCount))
        .append("upserted", upserted);
  }

  private WriteRequest.Type convertPayloadType(SplittablePayload.Type type) {
    switch (type) {
      case INSERT:
        return INSERT;
      case UPDATE:
        return UPDATE;
      case REPLACE:
        return REPLACE;
      case DELETE:
        return DELETE;
    }
    throw new IllegalArgumentException("Don't know how to convert SplittablePayload.Type " + type + " into WriteRequest.Type");
  }

  private boolean getBooleanOrFalse(BsonDocument doc, String limit) {
    return doc.getBoolean(limit, BsonBoolean.FALSE).getValue();
  }

  private String getString(BsonDocument doc, String keyName) {
    return doc.get(keyName).asString().getValue();
  }

  private WriteConcern getWriteConcern(DB db, String collectionName) {
    return db.getCollection(collectionName).getWriteConcern();
  }

  private List<BsonDocument> consumePayload(SplittablePayload payload) {
    List<BsonDocument> docs = payload.getPayload();
    // mark whole payload as consumed
    payload.setPosition(docs.size());
    return docs;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T command(String database, BsonDocument command, boolean slaveOk, FieldNameValidator fieldNameValidator, Decoder<T> commandResultDecoder) {
    final FongoDB db = fongo.getDB(database);
    LOG.debug("command() database:{}, command:{}", database, command);
    if (command.containsKey("create")) {
      final FongoDBCollection dbCollection = (FongoDBCollection) db.getCollection(command.get("create").asString().getValue());

      return (T) new BsonDocument("ok", BsonBoolean.TRUE);
    } else if (command.containsKey("count")) {
      final FongoDBCollection dbCollection = (FongoDBCollection) db.getCollection(command.get("count").asString().getValue());
      final DBObject query = dbObject(command, "query");
      final long limit = command.containsKey("limit") ? command.getInt64("limit").longValue() : -1;
      final long skip = command.containsKey("skip") ? command.getInt64("skip").longValue() : 0;

      return (T) new BsonDocument("n", new BsonDouble(dbCollection.getCount(query, null, limit, skip, dbCollection.getReadPreference(), 0, TimeUnit.MICROSECONDS, null)));
    } else if (command.containsKey("findandmodify") || command.containsKey("findAndModify")) {
      final String collectionName = command.containsKey("findandmodify")
          ? getString(command, "findandmodify")
          : getString(command, "findAndModify");
      final DBCollection dbCollection = db.getCollection(collectionName);
      final DBObject query = dbObject(command, "query");
      final DBObject update = dbObject(command, "update");
      final DBObject fields = dbObject(command, "fields");
      final DBObject sort = dbObject(command, "sort");
      final boolean returnNew = BsonBoolean.TRUE.equals(command.getBoolean("new", BsonBoolean.FALSE));
      final boolean upsert = BsonBoolean.TRUE.equals(command.getBoolean("upsert", BsonBoolean.FALSE));
      final boolean remove = BsonBoolean.TRUE.equals(command.getBoolean("remove", BsonBoolean.FALSE));

      if (update != null) {
        final FieldNameValidator validatorUpdate = fieldNameValidator.getValidatorForField("update");
        for (String updateName : update.keySet()) {
          if (!validatorUpdate.validate(updateName)) {
            throw new IllegalArgumentException("Invalid BSON field name " + updateName);
          }
        }
      }

      final DBObject andModify = dbCollection.findAndModify(query, fields, sort, remove, update, returnNew, upsert);
      return reencode(commandResultDecoder, "value", andModify);
    } else if (command.containsKey("distinct")) {
      final DBCollection dbCollection = db.getCollection(command.get("distinct").asString().getValue());
      final DBObject query = dbObject(command, "query");
      final List<Object> distincts = dbCollection.distinct(command.getString("key").getValue(), query);
      return reencode(commandResultDecoder, "values", bsonArray(distincts));
    } else if (command.containsKey("aggregate")) {
      final DBCollection dbCollection = db.getCollection(command.get("aggregate").asString().getValue());
      final AggregationOutput aggregate = dbCollection.aggregate(dbObjects(command, "pipeline"));
      final boolean v3 = command.containsKey("cursor");
      final String resultField = v3 ? "cursor" : "result";
      final Iterable<DBObject> results = aggregate.results();
      if (!v3) {
        return reencode(commandResultDecoder, resultField, results);
      } else {
        return reencode(commandResultDecoder, "cursor", new BasicDBObject("id", 0L).append("ns", dbCollection.getFullName()).append("firstBatch", results));
      }
    } else if (command.containsKey("renameCollection")) {
      db.renameCollection(command.getString("renameCollection").getValue(), command.getString("to").getValue(), command.getBoolean("dropTarget", BsonBoolean.FALSE).getValue());
      return (T) new BsonDocument("ok", BsonBoolean.TRUE);
    } else if (command.containsKey("createIndexes")) {
      final DBCollection dbCollection = db.getCollection(command.get("createIndexes").asString().getValue());
      final List<BsonValue> indexes = command.getArray("indexes").getValues();
      for (BsonValue indexBson : indexes) {
        final BsonDocument bsonDocument = indexBson.asDocument();
        DBObject keys = dbObject(bsonDocument.getDocument("key"));
        String name = bsonDocument.getString("name").getValue();
        boolean unique = bsonDocument.getBoolean("unique", BsonBoolean.FALSE).getValue();

        dbCollection.createIndex(keys, name, unique);
      }

      return (T) new BsonDocument("ok", BsonBoolean.TRUE);
    } else if (command.containsKey("drop")) {
      final DBCollection dbCollection = db.getCollection(command.get("drop").asString().getValue());
      dbCollection.drop();
      return (T) new BsonDocument("ok", BsonBoolean.TRUE);
    } else if (command.containsKey("dropIndexes")) {
      return executeDropIndexesCommand(db, command);
    } else if (command.containsKey("listIndexes")) {
      final DBCollection dbCollection = db.getCollection(command.get("listIndexes").asString().getValue());

      final BasicDBObject cmd = new BasicDBObject();
      cmd.put("ns", dbCollection.getFullName());

      final DBCursor cur = dbCollection.getDB().getCollection("system.indexes").find(cmd);

      final List<Document> each = documents(cur.toArray());
      return (T) new BsonDocument("cursor", new BsonDocument("id",
          new BsonInt64(0)).append("ns", new BsonString(dbCollection.getFullName()))
          .append("firstBatch", FongoBsonArrayWrapper.bsonArrayWrapper(each)));
    } else if (command.containsKey("listCollections")) {
      final List<DBObject> result = new ArrayList<DBObject>();
      for (final String name : db.getCollectionNames()) {
        result.add(new BasicDBObject("name", name).append("options", new BasicDBObject()));
      }
      return reencode(commandResultDecoder, "cursor", new BasicDBObject("id", 0L).append("ns", db.getName() + ".dontkown").append("firstBatch", result));
    } else if (command.containsKey("dropDatabase")) {
      db.dropDatabase();
      return (T) new BsonDocument("ok", new BsonInt32(1));
    } else if (command.containsKey("ping")) {
      return (T) new Document("ok", 1.0);
    } else if (command.containsKey("insert")) {
      final FongoDBCollection dbCollection = (FongoDBCollection) db.getCollection(command.get("insert").asString().getValue());
      List<BsonValue> documentsToInsert = command.getArray("documents").getValues();
      for (BsonValue document : documentsToInsert) {
        dbCollection.insert(dbObject(document.asDocument()));
      }
      return (T) new Document("ok", 1).append("n", documentsToInsert.size());
    } else if (command.containsKey("delete")) {
      final FongoDBCollection dbCollection = (FongoDBCollection) db.getCollection(command.get("delete").asString().getValue());
      List<BsonValue> documentsToDelete = command.getArray("deletes").getValues();
      for (BsonValue document : documentsToDelete) {
        if (!document.asDocument().containsKey("limit")) {
          throw new MongoCommandException(new BsonDocument("ok", BsonBoolean.FALSE).append("code", new BsonInt32(9)), this.fongo.getServerAddress());
        }
      }

      int numDocsDeleted = 0;
      for (BsonValue document : documentsToDelete) {
        BsonDocument deletesDocument = document.asDocument();

        DBObject deleteQuery = dbObject(deletesDocument.get("q").asDocument());

        BsonInt32 limit = deletesDocument.getInt32("limit");

        WriteResult result = null;
        if (limit.intValue() < 1) {
          result = dbCollection.remove(deleteQuery);
        } else {
          Iterator<DBObject> iterator = dbCollection.find(deleteQuery).limit(1).iterator();

          if (iterator.hasNext()) {
            DBObject docToDelete = iterator.next();
            result = dbCollection.remove(new BasicDBObject("_id", docToDelete.get("_id")));
          }
        }

        if (result != null) {
          numDocsDeleted += result.getN();
        }
      }
      return (T) new Document("ok", 1).append("n", numDocsDeleted);
    } else if (command.containsKey("find")) {
      final FongoDBCollection dbCollection = (FongoDBCollection) db.getCollection(command.get("find").asString().getValue());
      BsonInt32 limit = getValue(command, "limit", -1);
      BsonInt32 skip = getValue(command, "skip", 0);
      BsonInt32 maxScan = getValue(command, "maxScan", Integer.MAX_VALUE);
      DBObject projection = null;
      if (command.containsKey("projection")) {
        projection = dbObject(command.getDocument("projection"));
      }
      DBObject query = new BasicDBObject();
      query.put("$query", dbObject(asDocument(command.get("filter"))));
      if (command.containsKey("sort")) {
        query.put("$orderby", dbObject(command.getDocument("sort")));
      }
      final DBCursor cur = dbCollection.find(query, projection);
      cur.limit(limit.getValue());
      cur.skip(skip.getValue());
      cur.maxScan(maxScan.getValue());
      final List<Document> each = documents(cur.toArray());
//      return (T) new BsonDocument("cursor", new BsonDocument("id",
//          new BsonInt64(0)).append("ns", new BsonString(dbCollection.getFullName()))
//          .append("firstBatch", FongoBsonArrayWrapper.bsonArrayWrapper(each)));
      return reencode(commandResultDecoder, "cursor", new BasicDBObject("id", 0L).append("ns", dbCollection.getFullName()).append("firstBatch", each));
    } else if (command.containsKey("listDatabases")) {
      final List<String> databaseNames = fongo.getDatabaseNames();
      final List<BsonDocument> documents = new ArrayList<BsonDocument>();
      for (String databaseName : databaseNames) {
        documents.add(new BsonDocument("name", new BsonString(databaseName)));
      }
      return (T) new BsonDocument("databases", FongoBsonArrayWrapper.bsonArrayWrapper(documents));
    } else if (command.containsKey("geoNear")) {
      // http://docs.mongodb.org/manual/reference/command/geoNear/
      // TODO : handle "num" (override limit)
      try {
        FongoDBCollection collection = db.getCollection(getString(command, "geoNear"));

        BsonArray near = command.getArray("near");
        Coordinate coordinate = GeoUtil.coordinate(new double[]{
            near.get(0).asDouble().doubleValue(),
            near.get(1).asDouble().doubleValue()
        });

        List<DBObject> result = collection.geoNear(
            coordinate,
            command.containsKey("query") ? ExpressionParser.toDbObject(command.get("query")) : null,
            command.containsKey("limit") ? command.get("limit").asNumber().intValue() : null,
            command.containsKey("maxDistance") ? command.get("maxDistance").asNumber().doubleValue() : null,
            getBooleanOrFalse(command, "spherical")
        );
        if (result == null) {
          return (T) new Document(db.notOkErrorResult("can't geoNear"));
        }
        CommandResult okResult = db.okResult();
        BasicDBList list = new BasicDBList();
        for (DBObject res : result) {
          list.add(toDocument((BasicDBObject) res));
        }
        okResult.put("results", list);
        return (T) new Document(okResult);
      } catch (MongoException me) {
        return (T) new BsonDocument("ok", new BsonDouble(0.0))
            .append("err", new BsonInt32(me.getCode()))
            .append("errmsg", new BsonString(me.getMessage()));
      }
    } else if (command.containsKey("buildInfo")) {
      return (T) Document.parse("{\n" +
              "\t\"version\" : \"3.6.4\",\n" +
              "\t\"gitVersion\" : \"d0181a711f7e7f39e60b5aeb1dc7097bf6ae5856\",\n" +
              "\t\"modules\" : [ ],\n" +
              "\t\"allocator\" : \"tcmalloc\",\n" +
              "\t\"javascriptEngine\" : \"mozjs\",\n" +
              "\t\"sysInfo\" : \"deprecated\",\n" +
              "\t\"versionArray\" : [\n" +
              "\t\t3,\n" +
              "\t\t6,\n" +
              "\t\t4,\n" +
              "\t\t0\n" +
              "\t],\n" +
              "\t\"openssl\" : {\n" +
              "\t\t\"running\" : \"OpenSSL 1.0.1t  3 May 2016\",\n" +
              "\t\t\"compiled\" : \"OpenSSL 1.0.1t  3 May 2016\"\n" +
              "\t},\n" +
              "\t\"buildEnvironment\" : {\n" +
              "\t\t\"distmod\" : \"debian81\",\n" +
              "\t\t\"distarch\" : \"x86_64\",\n" +
              "\t\t\"cc\" : \"/opt/mongodbtoolchain/v2/bin/gcc: gcc (GCC) 5.4.0\",\n" +
              "\t\t\"ccflags\" : \"-fno-omit-frame-pointer -fno-strict-aliasing -ggdb -pthread -Wall -Wsign-compare -Wno-unknown-pragmas -Winvalid-pch -Werror -O2 -Wno-unused-local-typedefs -Wno-unused-function -Wno-deprecated-declarations -Wno-unused-but-set-variable -Wno-missing-braces -fstack-protector-strong -fno-builtin-memcmp\",\n" +
              "\t\t\"cxx\" : \"/opt/mongodbtoolchain/v2/bin/g++: g++ (GCC) 5.4.0\",\n" +
              "\t\t\"cxxflags\" : \"-Woverloaded-virtual -Wno-maybe-uninitialized -std=c++14\",\n" +
              "\t\t\"linkflags\" : \"-pthread -Wl,-z,now -rdynamic -Wl,--fatal-warnings -fstack-protector-strong -fuse-ld=gold -Wl,--build-id -Wl,--hash-style=gnu -Wl,-z,noexecstack -Wl,--warn-execstack -Wl,-z,relro\",\n" +
              "\t\t\"target_arch\" : \"x86_64\",\n" +
              "\t\t\"target_os\" : \"linux\"\n" +
              "\t},\n" +
              "\t\"bits\" : 64,\n" +
              "\t\"debug\" : false,\n" +
              "\t\"maxBsonObjectSize\" : 16777216,\n" +
              "\t\"storageEngines\" : [\n" +
              "\t\t\"devnull\",\n" +
              "\t\t\"ephemeralForTest\",\n" +
              "\t\t\"mmapv1\",\n" +
              "\t\t\"wiredTiger\"\n" +
              "\t],\n" +
              "\t\"ok\" : 1\n" +
              "}\n");
    } else {
      LOG.warn("Command not implemented: {}", command);
      throw new FongoException("Not implemented for command : " + JSON.serialize(dbObject(command)));
    }
  }

  private <T> T executeDropIndexesCommand(FongoDB db, BsonDocument command) {
    FongoDBCollection dbCollection = db.getCollection(command.get("dropIndexes").asString().getValue());
    BasicDBObject indexToDrop = new BasicDBObject(command.get("index").asDocument());
    for (IndexAbstract index : dbCollection.getIndexes()) {
      if (index.getKeys().equals(indexToDrop) || index.getKeys().equals(withZeroForId(indexToDrop))) {
        dbCollection.dropIndex(index.getName());
        return (T) new BsonDocument("ok", new BsonInt32(1));
      }
    }

    // will throw the expected exception
    dbCollection.dropIndex(dbCollection.createIndexNameFromKeys(indexToDrop));
    return null;
  }

  private DBObject withZeroForId(DBObject o) {
    DBObject result = new BasicDBObject("_id", 0);
    result.putAll(o.toMap());
    return result;
  }

  private BsonDocument asDocument(BsonValue value) {
    return value != null ? value.asDocument() : null;
  }

  private BsonInt32 getValue(BsonDocument command, String maxScan2, int value) {
    BsonInt32 maxScan;
    if (command.containsKey(maxScan2)) {
      maxScan = command.getInt32(maxScan2);
    } else {
      maxScan = new BsonInt32(value);
    }
    return maxScan;
  }

  private List<Document> documents(Iterable<DBObject> list) {
    // TODO : better way.
    final Codec<Document> documentCodec = MongoClient.getDefaultCodecRegistry().get(Document.class);
    final List<Document> each = new ArrayList<Document>();
    for (DBObject result : list) {
      final Document decode = documentCodec.decode(new BsonDocumentReader(bsonDocument(result)),
          decoderContext());
      each.add(decode);
    }
    return each;
  }

  private <T> T reencode(final Decoder<T> commandResultDecoder, final String resultField, final Iterable<DBObject> results) {
    return reencode(commandResultDecoder, resultField, new BsonArray(bsonDocuments(results)));
  }

  private <T> T reencode(final Decoder<T> commandResultDecoder, final String resultField, final BsonArray results) {
    return commandResultDecoder.decode(new BsonDocumentReader(new BsonDocument(resultField, results)), decoderContext());
  }

  private <T> T reencode(final Decoder<T> commandResultDecoder, final String resultField, final DBObject result) {
    final BsonValue value;
    if (result == null) {
      value = new BsonNull();
    } else {
      value = bsonDocument(result);
    }
    return commandResultDecoder.decode(new BsonDocumentReader(new BsonDocument(resultField, value)), decoderContext());
  }

  @Override
  public <T> QueryResult<T> query(MongoNamespace namespace, BsonDocument queryDocument, BsonDocument fields, int numberToReturn, int skip, boolean slaveOk, boolean tailableCursor, boolean awaitData, boolean noCursorTimeout, boolean partial, boolean oplogReplay, Decoder<T> resultDecoder) {
    LOG.debug("query() namespace:{} queryDocument:{}, fields:{}", namespace, queryDocument, fields);
    final DBCollection collection = dbCollection(namespace);

    final List<DBObject> objects = collection
        .find(dbObject(queryDocument), dbObject(fields))
        .limit(numberToReturn)
        .skip(skip)
        .toArray();

    return new QueryResult(namespace, decode(objects, resultDecoder), 1, fongo.getServerAddress());
  }

  @Override
  public <T> QueryResult<T> query(MongoNamespace namespace, BsonDocument queryDocument, BsonDocument fields, int skip,
                                  int limit, int batchSize, boolean slaveOk, boolean tailableCursor, boolean awaitData,
                                  boolean noCursorTimeout, boolean partial, boolean oplogReplay, Decoder<T> resultDecoder) {
    // we ignore the batchSize here since batching is not implemented.
    return query(namespace, queryDocument, fields,
        limit, skip, slaveOk, tailableCursor, awaitData,
        noCursorTimeout, partial, oplogReplay, resultDecoder);
  }

  @Override
  public <T> QueryResult<T> getMore(MongoNamespace namespace, long cursorId, int numberToReturn, Decoder<T> resultDecoder) {
    LOG.debug("getMore() namespace:{} cursorId:{}", namespace, cursorId);
    // 0 means Cursor exhausted.
    return new QueryResult(namespace, Collections.emptyList(), 0, fongo.getServerAddress());
  }

  @Override
  public void killCursor(List<Long> cursors) {
    LOG.info("killCursor() cursors:{}", cursors);
  }

  @Override
  public void killCursor(MongoNamespace namespace, List<Long> cursors) {
    LOG.debug("killCursor() namespace:{}, cursors:{}", namespace.getFullName(), cursors);
  }

  @Override
  public int getCount() {
    LOG.info("getCount()");
    return 0;
  }

  @Override
  public void release() {
    LOG.debug("release()");
  }

  private FongoDBCollection dbCollection(MongoNamespace namespace) {
    return fongo.getDB(namespace.getDatabaseName()).getCollection(namespace.getCollectionName());
  }

  private BulkWriteResult bulkWriteResult(com.mongodb.BulkWriteResult bulkWriteResult) {
    if (!bulkWriteResult.isAcknowledged()) {
      return BulkWriteResult.unacknowledged();
    }
    return BulkWriteResult.acknowledged(bulkWriteResult.getInsertedCount(), bulkWriteResult.getMatchedCount(), bulkWriteResult.getRemovedCount(), bulkWriteResult.getModifiedCount(), FongoDBCollection.translateBulkWriteUpsertsToNew(bulkWriteResult.getUpserts()));
  }

}
