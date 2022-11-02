"use strict";

const should = require("should");

const MongoQueryTranslator = require("../../../../lib/service/storage/mongo/MongoQueryTranslator");

describe("MongoQueryTranslator", () => {
  const translator = new MongoQueryTranslator();

  describe("translateClause", () => {
    it('can translate the clause "equals"', () => {
      const clause = {
        equals: { city: "Istanbul" },
      };

      const mongoClause = translator.translateClause(
        ...Object.entries(clause)[0]
      );

      should(mongoClause).be.eql({
        city: { "$eq": "Istanbul" },
      });
    });

    it('can translate the clause "in"', () => {
      const clause = {
        in: { city: ["Istanbul", "Tirana"] },
      };

      const mongoClause = translator.translateClause(
        ...Object.entries(clause)[0]
      );

      should(mongoClause).be.eql({
        city: { "$in": ["Istanbul", "Tirana"] },
      });
    });

    it('can translate the clause "exists"', () => {
      const clause = {
        exists: "city",
      };

      const mongoClause = translator.translateClause(
        ...Object.entries(clause)[0]
      );

      should(mongoClause).be.eql({
        city: { "$exists": true },
      });
    });

    it('can translate the clause "ids"', () => {
      const clause = {
        ids: {
          values: ["aschen", "melis"],
        },
      };

      const mongoClause = translator.translateClause(
        ...Object.entries(clause)[0]
      );

      should(mongoClause).be.eql({
        _id: { "$in": ["aschen", "melis"] }
      });
    });

    it('can translate the clause "missing"', () => {
      const clause = {
        missing: "city",
      };

      const mongoClause = translator.translateClause(
        ...Object.entries(clause)[0]
      );

      should(mongoClause).be.eql({
        city: { "$exists": false },
      });
    });

    it('can translate the clause "range"', () => {
      const clause = {
        range: {
          age: { gt: 25, gte: 25, lt: 27, lte: 27 },
        },
      };

      const mongoClause = translator.translateClause(
        ...Object.entries(clause)[0]
      );

      should(mongoClause).be.eql({
        age: { "$gt": 25, "$gte": 25, "$lt": 27, "$lte": 27 },
      });
    });
  });

  describe("translateOperator", () => {
    it('can translate operator "and"', () => {
      const operator = {
        and: [],
      };

      const mongoOperator = translator.translateOperator(
        ...Object.entries(operator)[0]
      );

      should(mongoOperator).be.eql({
        "$and": []
      });
    });

    it('can translate operators "or"', () => {
      const operator = {
        or: [],
      };

      const mongoOperator = translator.translateOperator(
        ...Object.entries(operator)[0]
      );

      should(mongoOperator).be.eql({
        "$or": []
      });
    });

    it('can translate operator "not"', () => {
      const operator = {
        not: { exists: "city" },
      };

      const mongoOperator = translator.translateOperator(
        ...Object.entries(operator)[0]
      );

      should(mongoOperator).be.eql({
        "$not": { "city": { "$exists": true } }
      });
    });
  });

  describe("translate", () => {
    it("can translate complexe filters", () => {
      const filters = {
        and: [
          { equals: { city: "Antalya" } },
          {
            not: {
              exists: "age",
            },
          },
        ],
      };

      const mongoQuery = translator.translate(filters);

      should(mongoQuery).be.eql({
        "$and": [
          { city: { "$eq": "Antalya" } },
          {
            "$not": {
              "age": { "$exists": true },
            },
          },
        ],
      });
    });

    it("can translate simple filters", () => {
      const filters = {
        equals: { city: "Istanbul" },
      };

      const mongoQuery = translator.translate(filters);

      should(mongoQuery).be.eql({
        city: { "$eq": "Istanbul" },
      });
    });
  });
});
