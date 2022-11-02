/*
 * Kuzzle, a backend software, self-hostable and ready to use
 * to power modern apps
 *
 * Copyright 2015-2022 Kuzzle
 * mailto: support AT kuzzle.io
 * website: http://kuzzle.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { JSONObject } from "kuzzle-sdk";

class KeywordError extends Error {
  public keyword: { name: string, type: string };

  constructor(type: string, name: string) {
    super(
      `The ${type} "${name}" of Koncorde DSL is not supported for search queries.`
    );

    this.keyword = { name, type };
  }
}
const KONCORDE_OPERATORS = ["and", "or", "not"];

const KONCORDE_CLAUSES_TO_ES = {
  equals: (content) => {
    const field = Object.keys(content)[0];
    const value = Object.values(content)[0];

    return {
      [field]: { "$eq": value },
    };
  },
  exists: (field) => {
    return {
      [field]: { "$exists": true },
    };
  },
  geoBoundingBox: null,
  geoDistance: null,
  geoDistanceRange: null,
  geoPolygon: null,
  ids: (content) => {
    return {
      _id: { "$in": content.values }
    };
  },
  in: (content) => {
    const field = Object.keys(content)[0];
    const value = Object.values(content)[0];

    return {
      [field]: { "$in": value }
    };
  },
  missing: (field) => {
    return {
      [field]: { "$exists": false },
    };
  },
  range: (content) => {
    const field = Object.keys(content)[0];
    const clause = { [field]: {} };

    for (const [op, value] of Object.entries(Object.values(content)[0])) {
      clause[field][`$${op}`] = value;
    }

    return clause;
  },
  regexp: null,
};

const KONCORDE_OPERATORS_TO_MONGO = {
  and: (content) => ({
    "$and": content,
  }),
  bool: undefined,
  not: (content) => ({
    "$not": content,
  }),
  or: (content) => ({
    "$or": content,
  }),
};

class QueryTranslator {
  translate(filters: JSONObject): JSONObject {
    const [name, value] = Object.entries(filters)[0];

    if (KONCORDE_OPERATORS.includes(name)) {
      return this.translateOperator(name, value);
    }

    return this.translateClause(name, value);
  }

  private translateOperator(operator, operands) {
    const converter = KONCORDE_OPERATORS_TO_MONGO[operator];

    if (converter === undefined) {
      throw new KeywordError("operator", operator);
    }

    if (operator === "not") {
      return converter(this.translate(operands));
    }

    const esOperands = [];

    for (const operand of operands) {
      esOperands.push(this.translate(operand));
    }

    return converter(esOperands);
  }

  private translateClause(clause, content) {
    const converter = KONCORDE_CLAUSES_TO_ES[clause];

    if (converter === undefined) {
      throw new KeywordError("clause", clause);
    }

    return converter(content);
  }
}

module.exports = QueryTranslator;
