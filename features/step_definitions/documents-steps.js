"use strict";

const http = require("http"),
  _ = require("lodash"),
  should = require("should"),
  { Given, Then } = require("cucumber");

Given(
  /I can( not)? create the following document:/,
  async function (not, dataTable) {
    const document = this.parseObject(dataTable);

    const index = document.index || this.props.index;
    const collection = document.collection || this.props.collection;

    await this.tryAction(
      this.sdk.document.create(index, collection, document.body, document._id),
      not,
      "Document should not have been created"
    );

    if (!not) {
      this.props.documentId = this.props.result._id;
    }
  }
);

Then(
  "The document {string} content match:",
  async function (documentId, dataTable) {
    const expectedContent = this.parseObject(dataTable);

    const document = await this.sdk.document.get(
      this.props.index,
      this.props.collection,
      documentId
    );

    for (const [key, value] of Object.entries(expectedContent)) {
      should(_.get(document._source, key)).be.eql(value);
    }
  }
);

Then(
  "I {string} the following multiple documents:",
  async function (action, dataTable) {
    action = `m${action[0].toUpperCase() + action.slice(1)}`;

    const documents = this.parseObjectArray(dataTable);

    this.props.result = await this.sdk.document[action](
      this.props.index,
      this.props.collection,
      documents
    );
  }
);

Then(
  /I execute the "(.*?)" action on the following documents:$/,
  async function (action, dataTable) {
    const documents = this.parseObjectArray(dataTable);

    const response = await this.sdk.query({
      controller: "document",
      action,
      index: this.props.index,
      collection: this.props.collection,
      body: { documents },
    });

    this.props.result = response.result;
  }
);

Then(
  "I {string} the document {string} with content:",
  async function (action, _id, dataTable) {
    const body = this.parseObject(dataTable);

    if (action === "create") {
      this.props.result = await this.sdk.document[action](
        this.props.index,
        this.props.collection,
        body,
        _id
      );
    } else {
      this.props.result = await this.sdk.document[action](
        this.props.index,
        this.props.collection,
        _id,
        body
      );
    }
  }
);

Then("I count {int} documents", async function (expectedCount) {
  const count = await this.sdk.document.count(
    this.props.index,
    this.props.collection
  );

  should(count).be.eql(expectedCount);
});

Then(
  "I count {int} documents matching:",
  async function (expectedCount, dataTable) {
    const properties = this.parseObject(dataTable);

    const query = {
      match: {
        ...properties,
      },
    };

    const count = await this.sdk.document.count(
      this.props.index,
      this.props.collection,
      { query }
    );

    should(count).be.eql(expectedCount);
  }
);

Then(/The document "(.*?)" should( not)? exist/, async function (id, not) {
  const exists = await this.sdk.document.exists(
    this.props.index,
    this.props.collection,
    id
  );

  if (not && exists) {
    throw new Error(`Document ${id} exists, but it shouldn't`);
  }

  if (!not && !exists) {
    throw new Error(`Expected document ${id} to exist`);
  }
});

Then(
  /I "(.*?)" the following document ids( with verb "(.*?)")?:/,
  async function (action, verb, dataTable) {
    const options = verb
      ? { verb, refresh: "wait_for" }
      : { refresh: "wait_for" };
    const ids = _.flatten(dataTable.rawTable).map(JSON.parse);

    this.props.result = await this.sdk.document[action](
      this.props.index,
      this.props.collection,
      ids,
      options
    );
  }
);

Then("I search documents with the following query:", function (queryRaw) {
  const query = JSON.parse(queryRaw);

  this.props.searchBody = { query };
});

Then(
  "I search documents with the following search body:",
  function (searchBodyRaw) {
    const searchBody = JSON.parse(searchBodyRaw);

    this.props.searchBody = searchBody;
  }
);

Then("with the following highlights:", function (highlightsRaw) {
  const highlights = JSON.parse(highlightsRaw);

  this.props.searchBody.highlight = highlights;
});

Then("with the following search options:", function (optionsRaw) {
  const options = JSON.parse(optionsRaw);

  this.props.searchOptions = options;
});

Then("I execute the search query", async function () {
  // temporary use of sdk.query until we add the new "remaining" property
  // in the SDK's SearchResults class
  const response = await this.sdk.query({
    action: "search",
    body: this.props.searchBody,
    collection: this.props.collection,
    controller: "document",
    index: this.props.index,
    ...this.props.searchOptions,
  });

  this.props.result = response.result;
});

Then("I execute the multisearch query:", async function (dataTable) {
  const targets = this.parseObjectArray(dataTable);
  // temporary use of sdk.query until we add the new "remaining" property
  // in the SDK's SearchResults class
  const response = await this.sdk.query({
    action: "search",
    targets,
    body: this.props.searchBody,
    controller: "document",
    ...this.props.searchOptions,
  });

  this.props.result = response.result;
});

Then("I scroll to the next page", async function () {
  // temporary use of raw results, until the "remaining" propery is made
  // available to the SearchResults SDK class
  if (!this.props.result.scrollId) {
    throw new Error("No scroll ID found");
  }

  const response = await this.sdk.query({
    action: "scroll",
    controller: "document",
    scroll: "30s",
    scrollId: this.props.result.scrollId,
  });

  this.props.result = response.result;
});

Then('I execute the search query with verb "GET"', async function () {
  const request = {
    action: "search",
    controller: "document",
    index: this.props.index,
    collection: this.props.collection,
  };
  const options = {};

  if (this.kuzzleConfig.PROTOCOL === "http") {
    request.searchBody = JSON.stringify(this.props.searchBody);
    options.verb = "GET";
  } else {
    request.body = this.props.searchBody;
  }
  const { result } = await this.sdk.query(request, options);
  this.props.result = result;
});

Then("I delete the document {string}", async function (id) {
  this.props.result = await this.sdk.document.delete(
    this.props.index,
    this.props.collection,
    id
  );
});

Then(
  "I export the collection {string}:{string} in the format {string}",
  async function (index, collection, format) {
    this.props.result = await new Promise((resolve, reject) => {
      const req = http.request(
        {
          hostname: this.host,
          port: this.port,
          path: `/${index}/${collection}/_export?format=${format}&size=1`,
          method: "GET",
        },
        (response) => {
          let data = [];

          response.on("data", (chunk) => {
            data.push(chunk.toString());
          });

          response.on("end", () => {
            resolve(data.join(""));
          });

          response.on("error", (error) => {
            reject(error);
          });
        }
      );

      req.end();
    });
  }
);

Then(
  "I export the collection {string}:{string} in the format {string}:",
  async function (index, collection, format, dataTable) {
    const options = this.parseObject(dataTable);
    this.props.result = await new Promise((resolve, reject) => {
      const req = http.request(
        {
          hostname: this.host,
          port: this.port,
          path: `/${index}/${collection}/_export?format=${format}&size=1`,
          method: "POST",
        },
        (response) => {
          let data = [];

          response.on("data", (chunk) => {
            data.push(chunk.toString());
          });

          response.on("end", () => {
            resolve(data.join(""));
          });

          response.on("error", (error) => {
            reject(error);
          });
        }
      );

      req.write(JSON.stringify(options));
      req.end();
    });
  }
);
