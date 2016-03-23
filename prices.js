/* eslint-disable no-console */
const MongoClient = require('mongodb').MongoClient;
const expect = require('chai').expect;

const url = 'mongodb://localhost:27017/shopgun-experiments-product';
const PRICES_COL = 'prices';

const runPricesExperiment = pricesCollection => {
  console.log('Running price experiment...');

  /**
   * We're given a list of price rules,
   * with prices that are overriden for some customer
   * regions.
   * A "*" means that the rule matches all regions.
   */
  const priceRules = [
    { productId: 'A', region: '*', price: 10 },
    { productId: 'A', region: 'FR', price: 20 },
    { productId: 'B', region: '*', price: 15 },
  ];

  /**
   * We want to get the prices for all products
   * for French customers, knowing that if several
   * price rules match for a product, we want to
   * take the most specific one into account.
   */
  const priceQuery = { region: 'FR' };

  /**
   * We expect the resulting price list to be the following:
   */
  const expectedPriceList = [
    { productId: 'A', price: 20 },
    { productId: 'B', price: 15 },
  ];

  /**
   * The next part sets the database up and performs
   * a mapReduce to find the correct price list.
   */

  function map() {
   /* global emit, query */
    const acceptRecord = function acceptRecord(specificity) {
      /* eslint-disable object-shorthand */
      emit(this.productId, { price: this.price, specificity: specificity });
    }.bind(this);

    if (this.region === query.region) {
      acceptRecord(2);
    } else if (this.region === '*') {
      acceptRecord(1);
    }
  }

  function reduce(key, values) {
    /* eslint-disable prefer-arrow-callback */
    return values.sort(function sortBySpecificity(a, b) {
      return b.specificity - a.specificity;
    })[0];
  }

  function finalize(key, reducedValue) {
    return {
      productId: key,
      price: reducedValue.price,
    };
  }

  return pricesCollection.insertMany(priceRules).then(() => {
    const options = { out: { inline: 1 }, scope: { query: priceQuery }, finalize };
    return pricesCollection.mapReduce(map, reduce, options);
  }).then(
    results => results.map(result => result.value)
  ).then(
    priceList => {
      expect(priceList).to.deep.equal(expectedPriceList);
      console.log('Successfully computed price list!');
      console.log(priceList);
    }
  );
};

MongoClient.connect(url)
  .then(
    db => {
      console.log('Connected to mongo server %s.', url);
      return db.collection(PRICES_COL).remove().then(
        () => console.log('Removed collection %s.', PRICES_COL)
      ).then(
        () => runPricesExperiment(db.collection(PRICES_COL))
      ).then(
        () => db.close()
      ).catch(err => {
        console.log(err);
        db.close();
      });
    }
  )
  .catch(err => {
    console.log(err);
  })
;
