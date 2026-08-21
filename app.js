// see https://github.com/mu-semtech/mu-javascript-template for more info
import { app, query, update, errorHandler } from 'mu';

const PREFIXES = `
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX gr: <http://purl.org/goodrelations/v1#>
  PREFIX schema: <http://schema.org/>
  PREFIX veeakker: <http://veeakker.be/vocabularies/shop/>
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
`;
app.get('/', function( req, res ) {
  res.send('Hello mu-javascript-template');
} );

// * business entities without constraints

/**
 * Checks whether there are any offerings left which are tied to a business which has no constraints.
 */
async function hasSuppliersWithoutConstraintsWhichHaveOfferings() {
  return (await query(`${PREFIXES}
    ASK {
      ?business a gr:BusinessEntity.
      FILTER NOT EXISTS {
        ?business ext:disallowedProductGroup ?group.
      }
      ?offering gr:availableAtOrFrom ?business.
  }`)).boolean;
}

/**
 * Removes a batch of relations from offering to a business which has no specified constraints.
 */
async function removeOfferingsAvailableAtOrFromForSuppliersWithoutConstraints() {
  await update(`${PREFIXES}
    DELETE {
      ?offering gr:availableAtOrFrom ?business.
    } WHERE {
      {
        SELECT DISTINCT ?business ?offering {
          ?business a gr:BusinessEntity.
          FILTER NOT EXISTS {
            ?business ext:disallowedProductGroup ?group.
          }
          ?offering gr:availableAtOrFrom ?business.
        } ORDER BY ?business LIMIT 100 # order by to work around Virtuoso bug
      }
    }`);
}

// * adding and removing business entities with configured rules

/* The tree of information for our constraint
 *
 * We want to ensure we have the businessEntity supplied iff:
 */
 //   ASK {
 //     ?business
 //       a gr:BusinessEntity.
 //
 //     ?offering
 //       a gr:Offering;
 //       gr:availableAtOrFrom ?business.
 //
 //     FILTER NOT EXISTS {
 //       ?offering gr:includesObject/gr:typeOfGood/^veeakker:hasProduct/skos:broader*/^ext:disallowedProductGroup ?business.
 //     }
 //   }
/*
 * The skos:broader* can be repaced with skos:broader? if there are only two levels.  We choose to first throw this
 * monster at the triplrostore and then see if it is too complex or not.  If this calculates, it's likely the most
 * preferred and future-proof solution.
 */


// * add business entities where needed

/*
 * We take an approach here in which we offload the full search to the triplestore.  We could split up this approach by
 * searching for BusinessEntity first and then processing each of those in turn.  That approach would provide more
 * insights to the administrators if things were to go wrong, but it would create more queries to inspect and be more
 * state to maintain.
 */

/**
 * Checks whether there are offerings which should have a businessEntity attached, but which do not.
 */
async function hasOfferingsWhichNeedExtraBusinessEntities() {
  return (await query(`${PREFIXES}
    ASK {
      ?business
        a gr:BusinessEntity.

      # Query early for result set limitation and require the filter to require binding.
      {
        SELECT DISTINCT ?business {
          ?business ext:disallowedProductGroup ?hasProductGroup.
        }
      }

      ?offering a gr:Offering.

      FILTER NOT EXISTS {
        ?offering gr:availableAtOrFrom ?business.
      }

      FILTER NOT EXISTS {
        ?offering gr:includesObject/gr:typeOfGood/^veeakker:hasProduct/skos:broader?/^ext:disallowedProductGroup ?business.
      }
    }`)).boolean;
}

/**
 * Adds a batch of incorrect relations from offering to business.
 */
async function addSuppliersForOfferingsWhichHaveNegativeConstraints() {
  return (await update(`${PREFIXES}
    INSERT {
      ?offering gr:availableAtOrFrom ?business.
    } WHERE {
     {
       SELECT DISTINCT ?business ?offering
       {
         ?business
          a gr:BusinessEntity.

        {
          SELECT DISTINCT ?business {
            ?business ext:disallowedProductGroup ?hasProductGroup.
          }
        }

        ?offering a gr:Offering.

        FILTER NOT EXISTS {
          ?offering gr:availableAtOrFrom ?business.
        }

        FILTER NOT EXISTS {
          ?offering gr:includesObject/gr:typeOfGood/^veeakker:hasProduct/skos:broader?/^ext:disallowedProductGroup ?business.
        }
      } ORDER BY ?business LIMIT 100 # order is not needed but works around a Virtuoso bug
    }
  }`));
}

// * remove business entities which are not allowed anymore
/**
 * Checks whether there are offerings which have a businessEntity attached but shouldn't.
 */
async function hasOfferingsWhichHaveExtraBusinessEntities() {
  return (await query(`${PREFIXES}
    ASK {
      ?business
        a gr:BusinessEntity.

      {
        SELECT DISTINCT ?business {
          ?business ext:disallowedProductGroup ?hasProductGroup.
        }
      }

      ?offering a gr:Offering;
        gr:availableAtOrFrom ?business.

      ?offering gr:includesObject/gr:typeOfGood/^veeakker:hasProduct/skos:broader?/^ext:disallowedProductGroup ?business.
    }`)).boolean;
}

async function removeSuppliersForOfferingsWhichLackNegativeConstraints() {
  await update(`${PREFIXES}
    DELETE {
      ?offering gr:availableAtOrFrom ?business.
    } WHERE {
     {
       SELECT DISTINCT ?business ?offering
       {
           ?business
             a gr:BusinessEntity.

           {
             SELECT DISTINCT ?business {
               ?business ext:disallowedProductGroup ?hasProductGroup.
             }
           }

           ?offering a gr:Offering;
             gr:availableAtOrFrom ?business.

           ?offering gr:includesObject/gr:typeOfGood/^veeakker:hasProduct/skos:broader?/^ext:disallowedProductGroup ?business.
        } ORDER BY ?business LIMIT 100  # order by to work around Virtuoso bug
      }
    }`);
}

/**
 * SHOP DISTRIBUTION
 *
 * We serve products which could be delivered based on:
 *
 * - veeakker:hasSupplier :: a shop mainly selects the products from a given supplier
 * - veeakker:hasDeliveryPlace :: the product must be available at a delivery place
 * - veeakker:disallowedProductGroup :: shops can disable whole product groups
 */

/**
 * Checks whether there are offering/shop links left for shops that have no constraints.
 */
async function hasShopOfferingsForUnconstrainedShops() {
  return (await query(`${PREFIXES}
    ASK {
      ?offering veeakker:offeredByShop ?shop.
      FILTER NOT EXISTS {
        {
          ?shop veeakker:hasDeliveryPlace ?any.
        } UNION {
          ?shop veeakker:disallowedProductGroup ?any.
        } UNION {
          ?shop veeakker:hasSupplier ?any.
        }
      }
    }`)).boolean;
}

/**
 * Removes a batch of offering/shop links for shops that have no constraints.
 */
async function removeShopOfferingsForUnconstrainedShops() {
  await update(`${PREFIXES}
    DELETE {
      ?offering veeakker:offeredByShop ?shop.
    } WHERE {
      {
        SELECT DISTINCT ?offering ?shop {
          ?offering veeakker:offeredByShop ?shop.
          FILTER NOT EXISTS {
            {
              ?shop veeakker:hasDeliveryPlace ?any.
            } UNION {
              ?shop veeakker:disallowedProductGroup ?any.
            } UNION {
              ?shop veeakker:hasSupplier ?any.
            }
          }
        } ORDER BY ?shop LIMIT 100 # order by to work around Virtuoso bug
      }
    }`);
}

// * remove shop links that violate the location constraint
//
// An offering qualifies for a shop when a supplier carrying it
// (gr:availableAtOrFrom) delivers to a delivery place the shop serves. Both
// shops and business entities use veeakker:hasDeliveryPlace, so the path
// veeakker:hasDeliveryPlace/^veeakker:hasDeliveryPlace/^gr:availableAtOrFrom
// connects shop → place → business → offering. The outer ?anyPlace ensures
// the constraint only applies to shops that actually have delivery places;
// shops without them are decided by the other dimensions.

/**
 * Checks whether there are offering/shop links which need removal for location constraint.
 */
async function hasShopOfferingsViolatingLocationConstraint() {
  return (await query(`${PREFIXES}
    ASK {
      ?offering veeakker:offeredByShop ?shop.
      ?shop veeakker:hasDeliveryPlace ?anyPlace.
      FILTER NOT EXISTS {
        ?shop veeakker:hasDeliveryPlace/^veeakker:hasDeliveryPlace/^gr:availableAtOrFrom ?offering.
      }
    }`)).boolean;
}

/**
 * Removes a batch of offering/shop links that violate the location constraint.
 */
async function removeShopOfferingsViolatingLocationConstraint() {
  await update(`${PREFIXES}
    DELETE {
      ?offering veeakker:offeredByShop ?shop.
    } WHERE {
      {
        SELECT DISTINCT ?offering ?shop {
          ?offering veeakker:offeredByShop ?shop.
          ?shop veeakker:hasDeliveryPlace ?anyPlace.
          FILTER NOT EXISTS {
            ?shop veeakker:hasDeliveryPlace/^veeakker:hasDeliveryPlace/^gr:availableAtOrFrom ?offering.
          }
        } ORDER BY ?shop LIMIT 100 # order by to work around Virtuoso bug
      }
    }`);
}

// * remove shop links that violate the product-group constraint

/**
 * Checks whether there are offering/shop links left where the offering's
 * product is in one of the shop's disallowed product groups.
 */
async function hasShopOfferingsViolatingProductGroupConstraint() {
  return (await query(`${PREFIXES}
    ASK {
      ?offering veeakker:offeredByShop ?shop.
      ?offering gr:includesObject/gr:typeOfGood/^veeakker:hasProduct/skos:broader?/^veeakker:disallowedProductGroup ?shop.
    }`)).boolean;
}

/**
 * Removes a batch of offering/shop links that violate the product-group constraint.
 */
async function removeShopOfferingsViolatingProductGroupConstraint() {
  await update(`${PREFIXES}
    DELETE {
      ?offering veeakker:offeredByShop ?shop.
    } WHERE {
      {
        SELECT DISTINCT ?offering ?shop {
          ?offering veeakker:offeredByShop ?shop.
          ?offering gr:includesObject/gr:typeOfGood/^veeakker:hasProduct/skos:broader?/^veeakker:disallowedProductGroup ?shop.
        } ORDER BY ?shop LIMIT 100 # order by to work around Virtuoso bug
      }
    }`);
}

// * remove shop links that violate the supplier constraint

/**
 * Checks whether there are offering/shop links left where the shop has a
 * supplier allow-list but the offering's supplier is not on it.
 */
async function hasShopOfferingsViolatingSupplierConstraint() {
  return (await query(`${PREFIXES}
    ASK {
      ?offering veeakker:offeredByShop ?shop.
      ?shop veeakker:hasSupplier ?anySupplier.
      FILTER NOT EXISTS {
        ?shop veeakker:hasSupplier/gr:offers ?offering.
      }
    }`)).boolean;
}

/**
 * Removes a batch of offering/shop links that violate the supplier constraint.
 */
async function removeShopOfferingsViolatingSupplierConstraint() {
  await update(`${PREFIXES}
    DELETE {
      ?offering veeakker:offeredByShop ?shop.
    } WHERE {
      {
        SELECT DISTINCT ?offering ?shop {
          ?offering veeakker:offeredByShop ?shop.
          ?shop veeakker:hasSupplier ?anySupplier.
          FILTER NOT EXISTS {
            ?shop veeakker:hasSupplier/gr:offers ?offering.
          }
        } ORDER BY ?shop LIMIT 100 # order by to work around Virtuoso bug
      }
    }`);
}

// * add shop links that satisfy all three constraints

/**
 * Checks whether there are offering/shop pairs that pass all three
 * constraints but are not linked yet.
 */
async function hasShopOfferingsWhichNeedLinks() {
  return (await query(`${PREFIXES}
    ASK {
      ?shop a veeakker:Shop.
      ?offering a gr:Offering.
      FILTER EXISTS {
        {
          ?shop veeakker:hasDeliveryPlace ?any.
        } UNION {
          ?shop veeakker:disallowedProductGroup ?any.
        } UNION {
          ?shop veeakker:hasSupplier ?any.
        }
      }
      FILTER NOT EXISTS {
        ?offering veeakker:offeredByShop ?shop.
      }
      FILTER NOT EXISTS {
        ?shop veeakker:hasDeliveryPlace ?anyPlace.
        FILTER NOT EXISTS {
          ?shop veeakker:hasDeliveryPlace/^veeakker:hasDeliveryPlace/^gr:availableAtOrFrom ?offering.
        }
      }
      FILTER NOT EXISTS {
        ?offering gr:includesObject/gr:typeOfGood/^veeakker:hasProduct/skos:broader?/^veeakker:disallowedProductGroup ?shop.
      }
      FILTER NOT EXISTS {
        ?shop veeakker:hasSupplier ?anySupplier.
        FILTER NOT EXISTS {
          ?shop veeakker:hasSupplier/gr:offers ?offering.
        }
      }
    }`)).boolean;
}

/**
 * Adds a batch of offering/shop links for pairs that satisfy all three
 * constraints. The nested FILTER NOT EXISTS expresses "no constraint set OR
 * constraint satisfied" for the positive allow-list (suppliers) and
 * positive-places (delivery-places) dimensions.
 */
async function addShopOfferingsWhichSatisfyConstraints() {
  await update(`${PREFIXES}
    INSERT {
      ?offering veeakker:offeredByShop ?shop.
    } WHERE {
      {
        SELECT DISTINCT ?offering ?shop {
          ?shop a veeakker:Shop.
          ?offering a gr:Offering.
          FILTER EXISTS {
            {
              ?shop veeakker:hasDeliveryPlace ?any.
            } UNION {
              ?shop veeakker:disallowedProductGroup ?any.
            } UNION {
              ?shop veeakker:hasSupplier ?any.
            }
          }
          FILTER NOT EXISTS {
            ?offering veeakker:offeredByShop ?shop.
          }
          FILTER NOT EXISTS {
            ?shop veeakker:hasDeliveryPlace ?anyPlace.
            FILTER NOT EXISTS {
              ?shop veeakker:hasDeliveryPlace/^veeakker:hasDeliveryPlace/^gr:availableAtOrFrom ?offering.
            }
          }
          FILTER NOT EXISTS {
            ?offering gr:includesObject/gr:typeOfGood/^veeakker:hasProduct/skos:broader?/^veeakker:disallowedProductGroup ?shop.
          }
          FILTER NOT EXISTS {
            ?shop veeakker:hasSupplier ?anySupplier.
            FILTER NOT EXISTS {
              ?shop veeakker:hasSupplier/gr:offers ?offering.
            }
          }
        } ORDER BY ?shop LIMIT 100 # order by to work around Virtuoso bug
      }
    }`);
}

async function distributeShops() {
  console.log("Starting distribution of offerings' shops");
  // remove links for shops that have no constraints on any dimension
  while ( await hasShopOfferingsForUnconstrainedShops() ) {
    await removeShopOfferingsForUnconstrainedShops();
  }
  while ( await hasShopOfferingsViolatingLocationConstraint() ) {
    await removeShopOfferingsViolatingLocationConstraint();
  }
  while ( await hasShopOfferingsViolatingProductGroupConstraint() ) {
    await removeShopOfferingsViolatingProductGroupConstraint();
  }
  while ( await hasShopOfferingsViolatingSupplierConstraint() ) {
    await removeShopOfferingsViolatingSupplierConstraint();
  }
  while ( await hasShopOfferingsWhichNeedLinks() ) {
    await addShopOfferingsWhichSatisfyConstraints();
  }
  console.log("Shop distribution complete.");
}

async function distributeLocations() {
  console.log("Starting distribution of offerings' locations");

  while ( await hasSuppliersWithoutConstraintsWhichHaveOfferings() ) {
    await removeOfferingsAvailableAtOrFromForSuppliersWithoutConstraints();
  }

  while ( await hasOfferingsWhichNeedExtraBusinessEntities() ) {
    await addSuppliersForOfferingsWhichHaveNegativeConstraints();
  }

  while ( await hasOfferingsWhichHaveExtraBusinessEntities() ) {
    await removeSuppliersForOfferingsWhichLackNegativeConstraints();
  }

  console.log("Location distribution complete.");
}

async function distributeAll() {
  await distributeLocations();
  await distributeShops();
}

app.post('/distribute', async function ( req, res ) {
  try {
    await distributeAll();
    res
      .status(200)
      .send(JSON.stringify({status: "ok"}));
  } catch (e) {
    console.error(`error ${e} occurred`);
    res
      .status(500)
      .send(JSON.stringify({status: "error", code: "500", message: e.stringify()}));
  }
});

app.post('/clean-suppliers-without-constraints', async (_req,res) => {
  try {
    // remove labeled suppliers which don't have constraints anymore
    while ( await hasSuppliersWithoutConstraintsWhichHaveOfferings() ) {
      await removeOfferingsAvailableAtOrFromForSuppliersWithoutConstraints();
    }
    res
      .status(200)
      .send(JSON.stringify({status: "ok"}));
  } catch (e) {
    console.error(`error ${e} occurred`);
    res
      .status(500)
      .send(JSON.stringify({status: "error", code: "500", message: e.stringify()}));
  }
});

app.post('/add-suppliers', async (_req,res) => {
  try {
    // add suppliers for labels which lack negative constraints
    while ( await hasOfferingsWhichNeedExtraBusinessEntities() ) {
      await addSuppliersForOfferingsWhichHaveNegativeConstraints();
    }
    res
      .status(200)
      .send(JSON.stringify({status: "ok"}));
  } catch (e) {
    console.error(`error ${e} occurred`);
    res
      .status(500)
      .send(JSON.stringify({status: "error", code: "500", message: e.stringify()}));
  }
});

app.post('/remove-suppliers', async (_req,res) => {
  try {
    // remove suppliers for labels which have negative constraints
    while ( await hasOfferingsWhichHaveExtraBusinessEntities() ) {
      await removeSuppliersForOfferingsWhichLackNegativeConstraints();
    }
    res
      .status(200)
      .send(JSON.stringify({status: "ok"}));
  } catch (e) {
    console.error(`error ${e} occurred`);
    res
      .status(500)
      .send(JSON.stringify({status: "error", code: "500", message: e.stringify()}));
  }
});

app.post('/distribute-shops', async (_req,res) => {
  try {
    await distributeShops();
    res
      .status(200)
      .send(JSON.stringify({status: "ok"}));
  } catch (e) {
    console.error(`error ${e} occurred`);
    res
      .status(500)
      .send(JSON.stringify({status: "error", code: "500", message: e.stringify()}));
  }
});

app.use(errorHandler);

// Run a full distribution pass on startup so the triplestore is consistent
// even when no delta messages are pending — e.g. after a restore, or while the
// service was down during triple changes. The pass is idempotent: each phase
// is guarded by an ASK query and is a no-op when there is nothing to do.
distributeAll().catch(e => console.error(`Startup distribution failed: ${e}`));
