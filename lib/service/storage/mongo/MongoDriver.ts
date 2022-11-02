import { CollectionMappings, JSONObject } from "kuzzle-sdk";
import { MongoClient, ObjectId } from "mongodb";

import { Service } from "../../service";
import * as kerrorLib from "../../../kerror";
import { flattenObject } from "../../../util/flattenObject";

const kerror = kerrorLib.wrap("services", "storage");

const FORBIDDEN_DATABASE_CHAR = "/\\. \"$*<>:|?";
const FORBIDDEN_COLLECTION_CHAR = "$"
const HIDDEN_COLLECTION = "_kuzzle_keep";

export class MongoDB extends Service {
  private client: MongoClient;
  private scope: string;

  constructor(config: JSONObject, scope: string) {
    super("mongodb", config);

    this.scope = scope;
  }

  async init(): Promise<void> {
    this.client = new MongoClient(this.config.client.uri);

    await this.client.connect();

    await this.client.db(this.scope).command({ ping: 1 });
  }

  translateKoncordeFilters(filters: JSONObject): JSONObject {
    throw Error("Not Implemented");
  }

  assertValidIndexAndCollection (index: string, collection?: string) {
    if (!this.checkDatabaseName(index)) {
      throw kerror.get("invalid_index_name", index);
    }

    if (collection&& !this.checkCollectionName(collection)) {
      throw kerror.get("invalid_collection_name", collection);
    }
  }

  private getDatabase (name: string) {
    return this.client.db(`${this.scope}@${name}`);
  }

  private getCollection (database: string, collection: string) {
    return this.getDatabase(database).collection(collection);
  }

  /**
   * Validate database name
   *
   * @see https://www.mongodb.com/docs/manual/reference/limits/#naming-restrictions
   */
  private checkDatabaseName (name: string): boolean {
    if (typeof name !== "string" || name.length === 0) {
      return false;
    }

    for (let i = 0; i < FORBIDDEN_DATABASE_CHAR.length; i++) {
      if (name.includes(FORBIDDEN_DATABASE_CHAR[i])) {
        return false;
      }
    }

    if (Buffer.from(name).length > 64) {
      return false;
    }

    return true;
  }

  /**
   * Validate collection name
   *
   * @see https://www.mongodb.com/docs/manual/reference/limits/#naming-restrictions
   */
  private checkCollectionName (name: string): boolean {
    if (typeof name !== "string" || name.length === 0) {
      return false;
    }

    if (name.startsWith('system.')) {
      return false;
    }

    for (let i = 0; i < FORBIDDEN_COLLECTION_CHAR.length; i++) {
      if (name.includes(FORBIDDEN_COLLECTION_CHAR[i])) {
        return false;
      }
    }

    if (Buffer.from(name).length > 100) {
      return false;
    }
  }

  async info(): Promise<any> {
    return {};
  }

  async stats(): Promise<{
    indexes: unknown[];
    size: number;
  }> {
    throw Error("Not Implemented");
  }
  async scroll(scrollId: any, opts: any): Promise<{
    aggregations: any;
    hits: {
      _id: string;
      _score: any;
      _source: any;
      collection: any;
      highlight: any;
      index: any;
      inner_hits: {};
    }[];
    remaining: any;
    scrollId: any;
    suggest: any;
    total: any;
  }> {
    throw Error("Not Implemented");
  }
  /**
   * Searches documents from elasticsearch with a query
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {Object} searchBody - Search request body (query, sort, etc.)
   * @param targets - contain index and collections for multisearch. If target is not null, index and collection sould be null.
   * @param {Object} options - from (undefined), size (undefined), scroll (undefined)
   *
   * @returns {Promise.<{ scrollId, hits, aggregations, suggest, total }>}
   */
  async search(
    {
      index,
      collection,
      searchBody,
      targets,
    }: {
      index: any;
      collection: any;
      searchBody: any;
      targets: any;
    },
    {
      from,
      size,
      scroll,
    }: {
      from: any;
      size: any;
      scroll: any;
    }
  ): Promise<{
    aggregations: any;
    hits: {
      _id: string;
      _score: any;
      _source: any;
      collection: any;
      highlight: any;
      index: any;
      inner_hits: {};
    }[];
    remaining: any;
    scrollId: any;
    suggest: any;
    total: any;
  }> {
    throw Error("Not Implemented");
  }
  /**
   * Gets the document with given ID
   *
   * @param index - Index name
   * @param collection - Collection name
   * @param id - Document ID
   *
   * @returns {Promise.<{ _id, _version, _source }>}
   */
  async get(index: string, collection: string, id: string): Promise<JSONObject> {
    const document = await this.getCollection(index, collection).findOne({ _id: getId(id) });

    if (document === null) {
      throw kerror.get("not_found", id, index, collection);
    }

    return {
      _id: id,
      _source: document,
      _version: null,
    };
  }
  /**
   * Returns the list of documents matching the ids given in the body param
   * NB: Due to internal Kuzzle mechanism, can only be called on a single
   * index/collection, using the body { ids: [.. } syntax.
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {Array.<String>} ids - Document IDs
   *
   * @returns {Promise.<{ items: Array<{ _id, _source, _version }>, errors }>}
   */
  mGet(
    index: any,
    collection: any,
    ids: any
  ): Promise<
    | {
        errors: any[];
        item: any[];
        items?: undefined;
      }
    | {
        errors: any[];
        items: any[];
        item?: undefined;
      }
  > {
    throw Error("Not Implemented");
  }
  /**
   * Counts how many documents match the filter given in body
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {Object} searchBody - Search request body (query, sort, etc.)
   *
   * @returns {Promise.<Number>} count
   */
  count(index: any, collection: any, searchBody?: {}): Promise<any> {
    throw Error("Not Implemented");
  }
  /**
   * Sends the new document to elasticsearch
   * Cleans data to match elasticsearch specifications
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {Object} content - Document content
   * @param {Object} options - id (undefined), refresh (undefined), userId (null)
   *
   * @returns {Promise.<Object>} { _id, _version, _source }
   */
  async create(
    index: string,
    collection: string,
    content: JSONObject,
    {
      id,
      refresh,
      userId,
    }: {
      id: string;
      refresh: any;
      userId?: string;
    }
  ): Promise<{
    _id: string;
    _source: JSONObject;
    _version: string;
  }> {
    const document: any = {
      ...content,
      _id: id ? getId(id) : undefined,
      _kuzzle_info: {
        author: getKuid(userId),
        createdAt: Date.now(),
        updatedAt: null,
        updater: null,
      }
    };

    const result = await this.getCollection(index, collection).insertOne(document);

    return {
      _id: result.insertedId.toString(),
      _source: document,
      _version: null,
    }
  }
  /**
   * Creates a new document to ElasticSearch, or replace it if it already exist
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {String} id - Document id
   * @param {Object} content - Document content
   * @param {Object} options - refresh (undefined), userId (null), injectKuzzleMeta (true)
   *
   * @returns {Promise.<Object>} { _id, _version, _source, created }
   */
  async createOrReplace(
    index: string,
    collection: string,
    id: string,
    content: JSONObject,
    {
      refresh,
      userId,
      injectKuzzleMeta,
    }: {
      refresh?: boolean;
      userId?: any;
      injectKuzzleMeta?: boolean;
    }
  ): Promise<{
    _id: string;
    _source: any;
    _version: any;
    created: boolean;
  }> {
    const changes = {
      ...content,
      ...(injectKuzzleMeta && {
        author: getKuid(userId),
        createdAt: Date.now(),
        updatedAt: null,
        updater: null,
      }),
    };

    const ret = await this.getCollection(index, collection).updateOne(
      {
        _id: getId(id),
      },
      { "$set": flattenObject(changes) },
      {
        upsert: true,
      }
    );

    const document = await this.get(index, collection, id);

    return {
      _id: ret.upsertedId.toString(),
      _source: document,
      _version: null,
      created: ret.upsertedCount === 1,
    };
  }
  /**
   * Sends the partial document to elasticsearch with the id to update
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {String} id - Document id
   * @param {Object} content - Updated content
   * @param {Object} options - refresh (undefined), userId (null), retryOnConflict (0)
   *
   * @returns {Promise.<{ _id, _version }>}
   */
  async update(
    index: string,
    collection: string,
    id: string,
    content: JSONObject,
    {
      refresh,
      userId,
      retryOnConflict,
    }: {
      refresh: any;
      userId?: any;
      retryOnConflict: any;
    }
  ): Promise<{
    _id: any;
    _source: any;
    _version: any;
  }> {
    const changes = {
      ...flattenObject(content),
      updatedAt: Date.now(),
      updater: getKuid(userId),
    };

    const ret = await this.getCollection(index, collection).updateOne(
      {
        _id: getId(id),
      },
      { "$set": changes },
    );

    const document = await this.get(index, collection, id);

    return {
      _id: ret.upsertedId,
      _source: document,
      _version: null,
    };
  }
  /**
   * Sends the partial document to elasticsearch with the id to update
   * Creates the document if it doesn't already exist
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {String} id - Document id
   * @param {Object} content - Updated content
   * @param {Object} options - defaultValues ({}), refresh (undefined), userId (null), retryOnConflict (0)
   *
   * @returns {Promise.<{ _id, _version }>}
   */
  upsert(
    index: any,
    collection: any,
    id: any,
    content: any,
    {
      defaultValues,
      refresh,
      userId,
      retryOnConflict,
    }: {
      defaultValues?: {};
      refresh: any;
      userId?: any;
      retryOnConflict: any;
    }
  ): Promise<{
    _id: any;
    _source: any;
    _version: any;
    created: boolean;
  }> {
    throw Error("Not Implemented");
  }
  /**
   * Replaces a document to ElasticSearch
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {String} id - Document id
   * @param {Object} content - Document content
   * @param {Object} options - refresh (undefined), userId (null)
   *
   * @returns {Promise.<{ _id, _version, _source }>}
   */
  replace(
    index: any,
    collection: any,
    id: any,
    content: any,
    {
      refresh,
      userId,
    }: {
      refresh: any;
      userId?: any;
    }
  ): Promise<{
    _id: any;
    _source: any;
    _version: any;
  }> {
    throw Error("Not Implemented");
  }
  /**
   * Sends to elasticsearch the document id to delete
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {String} id - Document id
   * @param {Object} options - refresh (undefined)
   *
   * @returns {Promise}
   */
  async delete(
    index: string,
    collection: string,
    id: string,
    {
      refresh,
    }: {
      refresh: any;
    }
  ): Promise<void> {
    const result = await this.getCollection(index, collection).deleteOne({
      _id: getId(id),
    });

    if (result.deletedCount === 0) {
      throw kerror.get("not_found", id, index, collection);
    }
  }

  /**
   * Deletes all documents matching the provided filters.
   * If fetch=false, the max documents write limit is not applied.
   *
   * Options:
   *  - size: size of the batch to retrieve documents (no-op if fetch=false)
   *  - refresh: refresh option for ES
   *  - fetch: if true, will fetch the documents before delete them
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {Object} query - Query to match documents
   * @param {Object} options - size (undefined), refresh (undefined), fetch (true)
   *
   * @returns {Promise.<{ documents, total, deleted, failures: Array<{ _shardId, reason }> }>}
   */
  deleteByQuery(
    index: any,
    collection: any,
    query: any,
    {
      refresh,
      size,
      fetch,
    }: {
      refresh: any;
      size?: number;
      fetch?: boolean;
    }
  ): Promise<{
    deleted: any;
    documents: any[];
    failures: any;
    total: any;
  }> {
    throw Error("Not Implemented");
  }

  /**
   * Delete fields of a document and replace it
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {String} id - Document id
   * @param {Array}  fields - Document fields to be removed
   * @param {Object} options - refresh (undefined), userId (null)
   *
   * @returns {Promise.<{ _id, _version, _source }>}
   */
  deleteFields(
    index: any,
    collection: any,
    id: any,
    fields: any,
    {
      refresh,
      userId,
    }: {
      refresh?: any;
      userId?: any;
    }
  ): Promise<{
    _id: any;
    _source: any;
    _version: any;
  }> {
    throw Error("Not Implemented");
  }

  /**
   * Updates all documents matching the provided filters
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {Object} query - Query to match documents
   * @param {Object} changes - Changes wanted on documents
   * @param {Object} options - refresh (undefined), size (undefined)
   *
   * @returns {Promise.<{ successes: [_id, _source, _status], errors: [ document, status, reason ] }>}
   */
  updateByQuery(
    index: any,
    collection: any,
    query: any,
    changes: any,
    {
      refresh,
      size,
      userId,
    }: {
      refresh?: any;
      size?: number;
      userId?: any;
    }
  ): Promise<{
    errors: any;
    successes: any;
  }> {
    throw Error("Not Implemented");
  }

  /**
   * Updates all documents matching the provided filters
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {Object} query - Query to match documents
   * @param {Object} changes - Changes wanted on documents
   * @param {Object} options - refresh (undefined)
   *
   * @returns {Promise.<{ successes: [_id, _source, _status], errors: [ document, status, reason ] }>}
   */
  bulkUpdateByQuery(
    index: any,
    collection: any,
    query: any,
    changes: any,
    {
      refresh,
    }: {
      refresh?: string;
    }
  ): Promise<{
    updated: any;
  }> {
    throw Error("Not Implemented");
  }

  /**
   * Execute the callback with a batch of documents of specified size until all
   * documents matched by the query have been processed.
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {Object} query - Query to match documents
   * @param {Function} callback - callback that will be called with the "hits" array
   * @param {Object} options - size (10), scrollTTL ('5s')
   *
   * @returns {Promise.<any[]>} Array of results returned by the callback
   */
  mExecute(
    index: any,
    collection: any,
    query: any,
    callback: any,
    {
      size,
      scrollTTl,
    }: {
      size?: number;
      scrollTTl?: string;
    }
  ): Promise<any[]> {
    throw Error("Not Implemented");
  }

  /**
   * Creates a new index.
   *
   * @param index - Index name
   */
  async createIndex(index: string): Promise<void> {
    this.assertValidIndexAndCollection(index);

    await this.getDatabase(index).createCollection(HIDDEN_COLLECTION );
  }

  /**
   * Creates an empty collection.
   * Mappings and settings will be applied if supplied.
   *
   * @param index - Index name
   * @param collection - Collection name
   * @param config - mappings ({}), settings ({})
   *
   * @returns {Promise}
   */
  async createCollection(
    index: string,
    collection: string,
    config: {
      mappings?: {
        _meta: any;
        dynamic: any;
        properties: any;
      };
      settings?: {};
    }
  ): Promise<void> {
    if (await this.hasCollection(index, collection)) {
      return;
    }

    await this.getDatabase(index).createCollection(collection);
  }
  /**
   * Retrieves settings definition for index/type
   *
   * @param index - Index name
   * @param collection - Collection name
   *
   * @returns {Promise.<{ settings }>}
   */
  async getSettings(index: string, collection: string): Promise<any> {
    return {};
  }
  /**
   * Retrieves mapping definition for index/type
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {Object} options - includeKuzzleMeta (false)
   *
   * @returns {Promise.<{ dynamic, _meta, properties }>}
   */
  getMapping(
    index: any,
    collection: any,
    {
      includeKuzzleMeta,
    }: {
      includeKuzzleMeta?: boolean;
    }
  ): Promise<CollectionMappings> {
    throw Error("Not Implemented");
  }
  /**
   * Updates a collection mappings and settings
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {Object} config - mappings ({}), settings ({})
   *
   * @returns {Promise}
   */
  updateCollection(
    index: any,
    collection: any,
    {
      mappings,
      settings,
    }: {
      mappings?: {
        _meta: any;
        dynamic: any;
        properties: any;
      };
      settings?: {};
    }
  ): Promise<any> {
    throw Error("Not Implemented");
  }
  /**
   * Given index settings we return a new version of index settings
   * only with allowed settings that can be set (during update or create index).
   * @param indexSettings the index settings
   * @returns {{index: *}} a new index settings with only allowed settings.
   */
  getAllowedIndexSettings(indexSettings: any): {
    index: _.Omit<any, "version" | "creation_date" | "provided_name" | "uuid">;
  } {
    throw Error("Not Implemented");
  }
  /**
   * Update a collection mappings
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {Object} mappings - Collection mappings in ES format
   *
   * @returns {Promise.<{ dynamic, _meta, properties }>}
   */
  updateMapping(
    index: any,
    collection: any,
    mappings: CollectionMappings
  ): Promise<{
    _meta: any;
    dynamic: any;
    properties: import("kuzzle-sdk").MappingsProperties;
  }> {
    throw Error("Not Implemented");
  }
  /**
   * Updates a collection settings (eg: analyzers)
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {Object} settings - Collection settings in ES format
   *
   * @returns {Promise}
   */
  updateSettings(index: any, collection: any, settings?: {}): Promise<any> {
    throw Error("Not Implemented");
  }
  /**
   * Empties the content of a collection. Keep the existing mapping and settings.
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   *
   * @returns {Promise}
   */
  truncateCollection(
    index: any,
    collection: any
  ): Promise<{
    deleted: any;
    documents: any[];
    failures: any;
    total: any;
  }> {
    throw Error("Not Implemented");
  }
  /**
   * Runs several action and document
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {Object[]} documents - Documents to import
   * @param {Object} options - timeout (undefined), refresh (undefined), userId (null)
   *
   * @returns {Promise.<{ items, errors }>
   */
  import(
    index: any,
    collection: any,
    documents: any,
    {
      refresh,
      timeout,
      userId,
    }: {
      refresh?: any;
      timeout?: any;
      userId?: any;
    }
  ): Promise<{
    errors: any[];
    items: any[];
  }> {
    throw Error("Not Implemented");
  }
  /**
   * Retrieves the complete list of existing collections in the current index
   *
   * @param {String} index - Index name
   * @param {Object.Boolean} includeHidden - Optional: include HIDDEN_COLLECTION in results
   *
   * @returns {Promise.<Array>} Collection names
   */
  listCollections(
    index: any,
    {
      includeHidden,
    }: {
      includeHidden?: boolean;
    }
  ): Promise<string[]> {
    throw Error("Not Implemented");
  }
  /**
   * Retrieves the complete list of indexes
   *
   * @returns {Promise.<Array>} Index names
   */
  listIndexes(includeVirtual?: boolean): Promise<string[]> {
    throw Error("Not Implemented");
  }
  /**
   * Returns an object containing the list of indexes and collections
   *
   * Record<index, collections>
   * @returns Record<string, string[]>
   */
  async getSchema(): Promise<Record<string, string[]>> {
    const schema: Record<string, string[]> = {};

    const { databases } = await this.client.db("admin").command({ listDatabases: 1 });

    const promises = [];

    for (const { name } of databases) {
      if (! name.includes(`${this.scope}@`)) {
        continue;
      }

      promises.push(
        this.client.db(name).listCollections().toArray()
          .then(collections => {
            schema[name.replace(`${this.scope}@`, '')] = collections.map(c => c.name)
          })
      );
    }

    await Promise.all(promises);

    return schema;
  }
  /**
   * Retrieves the complete list of aliases
   *
   * @returns {Promise.<Object[]>} [ { alias, index, collection, indice } ]
   */
  listAliases(): Promise<any[]> {
    throw Error("Not Implemented");
  }
  /**
   * Deletes a collection
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   *
   * @returns {Promise}
   */
  deleteCollection(index: any, collection: any): Promise<any> {
    throw Error("Not Implemented");
  }
  /**
   * Deletes multiple indexes
   *
   * @param {String[]} indexes - Index names
   *
   * @returns {Promise.<String[]>}
   */
  deleteIndexes(indexes?: any[]): Promise<string[]> {
    throw Error("Not Implemented");
  }
  /**
   * Deletes an index
   *
   * @param {String} index - Index name
   *
   * @returns {Promise}
   */
  deleteIndex(index: any): Promise<any> {
    throw Error("Not Implemented");
  }
  /**
   * Forces a refresh on the collection.
   *
   * /!\ Can lead to some performance issues.
   * cf https://www.elastic.co/guide/en/elasticsearch/guide/current/near-real-time.html for more details
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   *
   * @returns {Promise.<Object>} { _shards }
   */
  refreshCollection(
    index: any,
    collection: any
  ): Promise<{
    _shards: any;
  }> {
    throw Error("Not Implemented");
  }
  /**
   * Returns true if the document exists
   *
   * @param index - Index name
   * @param collection - Collection name
   * @param id - Document ID
   */
  async exists(index: string, collection: string, id: string): Promise<boolean> {
    const document = await this.getCollection(index, collection).findOne({
      _id: ObjectId.isValid(id) ? new ObjectId(id): id
    });

    return document !== null;
  }

  /**
   * Returns the list of documents existing with the ids given in the body param
   * NB: Due to internal Kuzzle mechanism, can only be called on a single
   * index/collection, using the body { ids: [.. } syntax.
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {Array.<String>} ids - Document IDs
   *
   * @returns {Promise.<{ items: Array<{ _id, _source, _version }>, errors }>}
   */
  mExists(
    index: any,
    collection: any,
    ids: any
  ): Promise<
    | {
        errors: any[];
        item: any[];
        items?: undefined;
      }
    | {
        errors: any[];
        items: any[];
        item?: undefined;
      }
  > {
    throw Error("Not Implemented");
  }
  /**
   * Returns true if the index exists
   *
   * @param {String} index - Index name
   *
   * @returns {Promise.<boolean>}
   */
  hasIndex(index: any, virtual: true): Promise<boolean> {
    throw Error("Not Implemented");
  }
  /**
   * Returns true if the collection exists
   *
   * @param index - Index name
   * @param collection - Collection name
   *
   * @returns {Promise.<boolean>}
   */
  async hasCollection(index: string, collection: string): Promise<boolean> {
    const collections = await this.getDatabase(index).listCollections().toArray();

    return Boolean(collections.find(c => c.name === collection));
  }
  /**
   * Returns true if the index has the hidden collection
   *
   * @param {String} index - Index name
   *
   * @returns {Promise.<boolean>}
   */
  private hasHiddenCollection;
  /**
   * Creates multiple documents at once.
   * If a content has no id, one is automatically generated and assigned to it.
   * If a content has a specified identifier, it is rejected if it already exists
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {Object[]} documents - Documents
   * @param {Object} options - timeout (undefined), refresh (undefined), userId (null)
   *
   * @returns {Promise.<Object>} { items, errors }
   */
  mCreate(
    index: any,
    collection: any,
    documents: any,
    {
      refresh,
      timeout,
      userId,
    }: {
      refresh?: any;
      timeout?: any;
      userId?: any;
    }
  ): Promise<any> {
    throw Error("Not Implemented");
  }
  /**
   * Creates or replaces multiple documents at once.
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {Object[]} documents - Documents
   * @param {Object} options - timeout (undefined), refresh (undefined), userId (null), injectKuzzleMeta (false), limits (true)
   *
   * @returns {Promise.<{ items, errors }>
   */
  mCreateOrReplace(
    index: any,
    collection: any,
    documents: any,
    {
      refresh,
      timeout,
      userId,
      injectKuzzleMeta,
      limits,
      source,
    }: {
      refresh?: any;
      timeout?: any;
      userId?: any;
      injectKuzzleMeta?: boolean;
      limits?: boolean;
      source?: boolean;
    }
  ): Promise<any> {
    throw Error("Not Implemented");
  }
  /**
   * Updates multiple documents with one request
   * Replacements are rejected if targeted documents do not exist
   * (like with the normal "update" method)
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {Object[]} documents - Documents
   * @param {Object} options - timeout (undefined), refresh (undefined), retryOnConflict (0), userId (null)
   *
   * @returns {Promise.<Object>} { items, errors }
   */
  mUpdate(
    index: any,
    collection: any,
    documents: any,
    {
      refresh,
      retryOnConflict,
      timeout,
      userId,
    }: {
      refresh: any;
      retryOnConflict?: number;
      timeout: any;
      userId?: any;
    }
  ): Promise<any> {
    throw Error("Not Implemented");
  }
  /**
   * Creates or replaces multiple documents at once.
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {Object[]} documents - Documents
   * @param {Object} options - refresh (undefined), retryOnConflict (0), timeout (undefined), userId (null)
   *
   * @returns {Promise.<{ items, errors }>
   */
  mUpsert(
    index: any,
    collection: any,
    documents: any,
    {
      refresh,
      retryOnConflict,
      timeout,
      userId,
    }: {
      refresh?: any;
      retryOnConflict?: number;
      timeout?: any;
      userId?: any;
    }
  ): Promise<any> {
    throw Error("Not Implemented");
  }
  /**
   * Replaces multiple documents at once.
   * Replacements are rejected if targeted documents do not exist
   * (like with the normal "replace" method)
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {Object[]} documents - Documents
   * @param {Object} options - timeout (undefined), refresh (undefined), userId (null)
   *
   * @returns {Promise.<Object>} { items, errors }
   */
  mReplace(
    index: any,
    collection: any,
    documents: any,
    {
      refresh,
      timeout,
      userId,
    }: {
      refresh?: any;
      timeout?: any;
      userId?: any;
    }
  ): Promise<any> {
    throw Error("Not Implemented");
  }
  /**
   * Deletes multiple documents with one request
   *
   * @param {String} index - Index name
   * @param {String} collection - Collection name
   * @param {Array.<String>} ids - Documents IDs
   * @param {Object} options - timeout (undefined), refresh (undefined)
   *
   * @returns {Promise.<{ documents, errors }>
   */
  mDelete(
    index: string,
    collection: string,
    ids: string[],
    {
      refresh,
    }: {
      refresh?: any;
    }
  ): Promise<{
    documents: any[];
    errors: any[];
  }> {
    throw Error("Not Implemented");
  }
}

function getKuid(userId: string) {
  return userId ? String(userId) : null;
}

function getId (id: string) {
  return ObjectId.isValid(id) ? new ObjectId(id): id;
}
