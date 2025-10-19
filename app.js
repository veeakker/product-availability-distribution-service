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
 * Removes (at most 100) relations from offering to a business which has no specified constraints.
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
        } LIMIT 100
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
        SELECT ?hasProductGroup {
          ?business ext:disallowedProductGroup ?hasProductGroup.
        } LIMIT 1
      }
      FILTER EXISTS {
        ?business ext:disallowedProductGroup ?hasProductGroup.
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
 * Adds (at most 100) incorrect relations from offering to business.
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
          SELECT ?hasProductGroup {
            ?business ext:disallowedProductGroup ?hasProductGroup.
          } LIMIT 1
        }
        FILTER EXISTS {
            ?business ext:disallowedProductGroup ?hasProductGroup.
        }

        ?offering a gr:Offering.

        FILTER NOT EXISTS {
          ?offering gr:availableAtOrFrom ?business.
        }

        FILTER NOT EXISTS {
          ?offering gr:includesObject/gr:typeOfGood/^veeakker:hasProduct/skos:broader?/^ext:disallowedProductGroup ?business.
        }
      } LIMIT 100
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
        SELECT ?hasProductGroup {
          ?business ext:disallowedProductGroup ?hasProductGroup.
        } LIMIT 1
      }
      FILTER EXISTS {
        ?business ext:disallowedProductGroup ?hasProductGroup.
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
             SELECT ?hasProductGroup {
               ?business ext:disallowedProductGroup ?hasProductGroup.
             } LIMIT 1
           }
           FILTER EXISTS {
             ?business ext:disallowedProductGroup ?hasProductGroup.
           }

           ?offering a gr:Offering;
             gr:availableAtOrFrom ?business.

           ?offering gr:includesObject/gr:typeOfGood/^veeakker:hasProduct/skos:broader?/^ext:disallowedProductGroup ?business.
        } LIMIT 100
      }
    }`);
}

app.post('/distribute', async function ( req, res ) {
  console.log("Starting distribution of offerings' locations");

  try {
    // remove labeled suppliers which don't have constraints anymore
    while ( await hasSuppliersWithoutConstraintsWhichHaveOfferings() ) {
      await removeOfferingsAvailableAtOrFromForSuppliersWithoutConstraints();
    }

    // add suppliers for labels which lack negative constraints
    while ( await hasOfferingsWhichNeedExtraBusinessEntities() ) {
      await addSuppliersForOfferingsWhichHaveNegativeConstraints();
    }

    // remove suppliers for labels which have negative constraints
    while ( await hasOfferingsWhichHaveExtraBusinessEntities() ) {
      await removeSuppliersForOfferingsWhichLackNegativeConstraints();
    }

    console.log("Location distribution complete.");
    res
      .status(200)
      .send(JSON.stringify(
        {
          status: "ok"
        }));
  } catch (e) {
    console.error(`error ${e} occurred`);
    res
      .status(500)
      .send(JSON.stringify(
        {
          status: "error",
          code: "500",
          message: e.stringify()
        }));
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
      .send(JSON.stringify(
        {
          status: "ok"
        }));
  } catch (e) {
    console.error(`error ${e} occurred`);
    res
      .status(500)
      .send(JSON.stringify(
        {
          status: "error",
          code: "500",
          message: e.stringify()
        }));
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
      .send(JSON.stringify(
        {
          status: "ok"
        }));
  } catch (e) {
    console.error(`error ${e} occurred`);
    res
      .status(500)
      .send(JSON.stringify(
        {
          status: "error",
          code: "500",
          message: e.stringify()
        }));
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
      .send(JSON.stringify(
        {
          status: "ok"
        }));
  } catch (e) {
    console.error(`error ${e} occurred`);
    res
      .status(500)
      .send(JSON.stringify(
        {
          status: "error",
          code: "500",
          message: e.stringify()
        }));
  }
});

app.use(errorHandler);
