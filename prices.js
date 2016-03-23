/* eslint-disable no-console */
const MongoClient = require('mongodb').MongoClient;

const url = 'mongodb://localhost:27017/shopgun-experiments-product';
const PRICES_COL = 'prices';

const runPricesExperiment = db => {
  console.log('Running price experiment...');

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

  const prices = db.collection(PRICES_COL);
  return prices.insertMany([
    { productId: 'A', region: '*', price: 10 },
    { productId: 'A', region: 'FR', price: 20 },
    { productId: 'B', region: '*', price: 15 },
  ]).then(() => {
    const query = { region: 'FR' };
    const options = { out: { inline: 1 }, scope: { query } };
    return prices.mapReduce(map, reduce, options);
  }).then(console.log);
};

MongoClient.connect(url)
  .then(
    db => {
      console.log('Connected to mongo server %s.', url);
      return db.collection(PRICES_COL).remove().then(
        () => console.log('Removed collection %s.', PRICES_COL)
      ).then(
        () => runPricesExperiment(db)
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
