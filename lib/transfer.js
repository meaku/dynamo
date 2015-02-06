// UTILITY FUNCTIONS
function transfer( schemas ) {
  
  return {

    to: function ( table, item )
    {
      var schema = schemas[ table ];

      function iterate( o ) {
        var payload;

        if( o instanceof Array )
        {
          payload = [];

          o.forEach( function( key ) {
            // GO DEEPER
            if( key instanceof Object ) payload.push( { "M": iterate( key ) } );
            if( key instanceof Array )  payload.push( { "L": iterate( key ) } );
          });

          return payload;
        }

        if( o instanceof Object )
        {
          payload = {};

          for( var key in o ) {
            // GO DEEPER
            if( o[key] instanceof Object ) payload[key] = { "M": iterate( o[key] ) };
            if( o[key] instanceof Array  ) payload[key] = { "L": iterate( o[key] ) };

            // SCHEMA ONLY OBJECT
            if( schema[ key ] == "BOOL" ) payload[key] = { "BOOL": Boolean( o[key] ) };
            if( schema[ key ] == "M" )    payload[key] = { "M": iterate( o[key] ) };
            if( schema[ key ] == "L" )    payload[key] = { "L": iterate( o[key] ) };
            if( schema[ key ] == "S" )    payload[key] = { "S": String( o[key] ) };
            if( schema[ key ] == "SS")    payload[key] = { "SS": o[key].map( function(e) { return String( e ); } ) };
            if( schema[ key ] == "N" )    payload[key] = { "N": String( o[key] ) };
            if( schema[ key ] == "NS")    payload[key] = { "NS": o[key].map( function(e) { return String( e ); } ) };
            if( schema[ key ] == "B" )    payload[key] = { "B": o[key].toString("base64") };
            if( schema[ key ] == "BS")    payload[key] = { "BS": o[key].map( function(e) { return e.toString("base64"); } ) };
          }

          return payload;
        }

      }

      return iterate( item );
    }
    ,
    from: function( table, item )
    {
      var schema = schemas[ table ];

      function iterate( o ) {
        var payload;

        if( o instanceof Array )
        {
          payload = [];

          o.forEach( function( key ) {
            // GO DEEPER
            if( key instanceof Object ) payload.push( iterate( key["M"] ) );
            if( key instanceof Array ) payload.push( iterate( key["L"] ) );
          });

          return payload;
        }

        if( o instanceof Object )
        {
          payload = {};

          for( var key in o ) {
              // SCHEMA
              if( o[key] && o[key]["BOOL"]) payload[key] = Boolean( o[key]["BOOL"] );
              if( o[key] && o[key]["M"]) payload[key] = iterate( o[key]["M"] );
              if( o[key] && o[key]["L"]) payload[key] = iterate( o[key]["L"] );
              if( o[key] && o[key]["S"]) payload[key] = String( o[key]["S"] );
              if( o[key] && o[key]["SS"]) payload[key] = o[key]["SS"].map( function(e) { return String( e ); });
              if( o[key] && o[key]["N"]) payload[key] = Number( o[key]["N"] );
              if( o[key] && o[key]["NS"]) payload[key] = o[key]["NS"].map( function(e) { return Number( e ); });
              if( o[key] && o[key]["B"]) payload[key] = new Buffer( o[key]["B"], "base64");
              if( o[key] && o[key]["BS"]) payload[key] = o[key]["BS"].map( function(e) { return new Buffer( e, "base64"); });
          }

          return payload;
        }
      }

      return iterate( item );
    }
  };
};

module.exports = transfer;
