var Minio = require('minio')
const { Pool, Client } = require('pg')
const uuidv4 = require ('uuid')
const cuid= require('cuid');
const slug = require('slugid')
require('dotenv').config()
require('events').EventEmitter.defaultMaxListeners = Infinity; 

// Using a pre-defined json because internet connectivity
  const json = 
  `
  [
    {
      "name": "SC Membership #0",
      "symbol": "SNOW",
      "description": "Test Description",
      "seller_fee_basis_points": 1000,
      "image": "https:/IMAGE_BASE_URL/0.png",
      "external_url": "https://snowcrash.com",
      "tokenId": 0,
      "attributes": [
        {
          "trait_type": "Face",
          "value": "White"
        },
        {
          "trait_type": "Nose",
          "value": "4"
        },
        {
          "trait_type": "Eyes",
          "value": "0.3"
        }
      ],
      "collection": {
        "name": "Demo1",
        "family": "Demo"
      },
      "properties": {
        "files": [
          {
            "uri": "https:/IMAGE_BASE_URL/0.png",
            "type": "image/png"
          }
        ],
        "category": "image",
        "creators": [
          {
            "address": "SC_WALLET",
            "share": 0.3,
            "verified": 1
          },
          {
            "address": "TAM_ADDRESS",
            "share": 0.7,
            "verified": 1
          }
        ],
        "collection_details": {
          "drop_price": 1,
          "primary_sales_royalty": 2,
          "secondary_market_royalty": 10,
          "drop_date": "Sat Jan 15 2022 03:00:00 GMT-0500 (Eastern Standard Time)"
        },
        "primary_contact": {
          "artist_username": "Tam",
          "artist_wallet_address": "TAM_ADDRESS",
          "artist_email": "tam@ay.com",
          "artist_phone_number ": "123456"
        }
      }
    },
    {
      "name": "SC Membership #1",
      "symbol": "SNOW",
      "description": "Test Description",
      "seller_fee_basis_points": 1000,
      "image": "https:/IMAGE_BASE_URL/1.png",
      "external_url": "https://snowcrash.com",
      "tokenId": 1,
      "attributes": [
        {
          "trait_type": "Face",
          "value": "White"
        },
        {
          "trait_type": "Nose",
          "value": "2"
        },
        {
          "trait_type": "Eyes",
          "value": "0.1"
        }
      ],
      "collection": {
        "name": "Demo1",
        "family": "Demo"
      },
      "properties": {
        "files": [
          {
            "uri": "https:/IMAGE_BASE_URL/1.png",
            "type": "image/png"
          }
        ],
        "category": "image",
        "creators": [
          {
            "address": "SC_WALLET",
            "share": 0.3,
            "verified": 1
          },
          {
            "address": "TAM_ADDRESS",
            "share": 0.7,
            "verified": 1
          }
        ],
        "collection_details": {
          "drop_price": 1,
          "primary_sales_royalty": 2,
          "secondary_market_royalty": 10,
          "drop_date": "Sat Jan 15 2022 03:00:00 GMT-0500 (Eastern Standard Time)"
        },
        "primary_contact": {
          "artist_username": "Tam",
          "artist_wallet_address": "TAM_ADDRESS",
          "artist_email": "tam@ay.com",
          "artist_phone_number ": "123456"
        }
      }
    },
    {
      "name": "SC Membership #2",
      "symbol": "SNOW",
      "description": "Test Description",
      "seller_fee_basis_points": 1000,
      "image": "https:/IMAGE_BASE_URL/2.png",
      "external_url": "https://snowcrash.com",
      "tokenId": 2,
      "attributes": [
        {
          "trait_type": "Face",
          "value": "Red"
        },
        {
          "trait_type": "Nose",
          "value": "3"
        },
        {
          "trait_type": "Eyes",
          "value": "0.1"
        }
      ],
      "collection": {
        "name": "Demo1",
        "family": "Demo"
      },
      "properties": {
        "files": [
          {
            "uri": "https:/IMAGE_BASE_URL/2.png",
            "type": "image/png"
          }
        ],
        "category": "image",
        "creators": [
          {
            "address": "SC_WALLET",
            "share": 0.3,
            "verified": 1
          },
          {
            "address": "TAM_ADDRESS",
            "share": 0.7,
            "verified": 1
          }
        ],
        "collection_details": {
          "drop_price": 1,
          "primary_sales_royalty": 2,
          "secondary_market_royalty": 10,
          "drop_date": "Sat Jan 15 2022 03:00:00 GMT-0500 (Eastern Standard Time)"
        },
        "primary_contact": {
          "artist_username": "Tam",
          "artist_wallet_address": "TAM_ADDRESS",
          "artist_email": "tam@ay.com",
          "artist_phone_number ": "123456"
        }
      }
    },
    {
      "name": "SC Membership #3",
      "symbol": "SNOW",
      "description": "Test Description",
      "seller_fee_basis_points": 1000,
      "image": "https:/IMAGE_BASE_URL/3.png",
      "external_url": "https://snowcrash.com",
      "tokenId": 3,
      "attributes": [
        {
          "trait_type": "Face",
          "value": "Blue"
        },
        {
          "trait_type": "Nose",
          "value": "4"
        },
        {
          "trait_type": "Eyes",
          "value": "0.1"
        }
      ],
      "collection": {
        "name": "Demo1",
        "family": "Demo"
      },
      "properties": {
        "files": [
          {
            "uri": "https:/IMAGE_BASE_URL/3.png",
            "type": "image/png"
          }
        ],
        "category": "image",
        "creators": [
          {
            "address": "SC_WALLET",
            "share": 0.3,
            "verified": 1
          },
          {
            "address": "TAM_ADDRESS",
            "share": 0.7,
            "verified": 1
          }
        ],
        "collection_details": {
          "drop_price": 1,
          "primary_sales_royalty": 2,
          "secondary_market_royalty": 10,
          "drop_date": "Sat Jan 15 2022 03:00:00 GMT-0500 (Eastern Standard Time)"
        },
        "primary_contact": {
          "artist_username": "Tam",
          "artist_wallet_address": "TAM_ADDRESS",
          "artist_email": "tam@ay.com",
          "artist_phone_number ": "123456"
        }
      }
    },
    {
      "name": "SC Membership #4",
      "symbol": "SNOW",
      "description": "Test Description",
      "seller_fee_basis_points": 1000,
      "image": "https:/IMAGE_BASE_URL/4.png",
      "external_url": "https://snowcrash.com",
      "tokenId": 4,
      "attributes": [
        {
          "trait_type": "Face",
          "value": "Pink"
        },
        {
          "trait_type": "Nose",
          "value": "3"
        },
        {
          "trait_type": "Eyes",
          "value": "0.1"
        }
      ],
      "collection": {
        "name": "Demo1",
        "family": "Demo"
      },
      "properties": {
        "files": [
          {
            "uri": "https:/IMAGE_BASE_URL/4.png",
            "type": "image/png"
          }
        ],
        "category": "image",
        "creators": [
          {
            "address": "SC_WALLET",
            "share": 0.3,
            "verified": 1
          },
          {
            "address": "TAM_ADDRESS",
            "share": 0.7,
            "verified": 1
          }
        ],
        "collection_details": {
          "drop_price": 1,
          "primary_sales_royalty": 2,
          "secondary_market_royalty": 10,
          "drop_date": "Sat Jan 15 2022 03:00:00 GMT-0500 (Eastern Standard Time)"
        },
        "primary_contact": {
          "artist_username": "Tam",
          "artist_wallet_address": "TAM_ADDRESS",
          "artist_email": "tam@ay.com",
          "artist_phone_number ": "123456"
        }
      }
    },
    {
      "name": "SC Membership #5",
      "symbol": "SNOW",
      "description": "Test Description",
      "seller_fee_basis_points": 1000,
      "image": "https:/IMAGE_BASE_URL/5.png",
      "external_url": "https://snowcrash.com",
      "tokenId": 5,
      "attributes": [
        {
          "trait_type": "Face",
          "value": "Green"
        },
        {
          "trait_type": "Nose",
          "value": "6"
        },
        {
          "trait_type": "Eyes",
          "value": "0.1"
        }
      ],
      "collection": {
        "name": "Demo1",
        "family": "Demo"
      },
      "properties": {
        "files": [
          {
            "uri": "https:/IMAGE_BASE_URL/5.png",
            "type": "image/png"
          }
        ],
        "category": "image",
        "creators": [
          {
            "address": "SC_WALLET",
            "share": 0.3,
            "verified": 1
          },
          {
            "address": "TAM_ADDRESS",
            "share": 0.7,
            "verified": 1
          }
        ],
        "collection_details": {
          "drop_price": 1,
          "primary_sales_royalty": 2,
          "secondary_market_royalty": 10,
          "drop_date": "Sat Jan 15 2022 03:00:00 GMT-0500 (Eastern Standard Time)"
        },
        "primary_contact": {
          "artist_username": "Tam",
          "artist_wallet_address": "TAM_ADDRESS",
          "artist_email": "tam@ay.com",
          "artist_phone_number ": "123456"
        }
      }
    },
    {
      "name": "SC Membership #6",
      "symbol": "SNOW",
      "description": "Test Description",
      "seller_fee_basis_points": 1000,
      "image": "https:/IMAGE_BASE_URL/6.png",
      "external_url": "https://snowcrash.com",
      "tokenId": 6,
      "attributes": [
        {
          "trait_type": "Face",
          "value": "Teal"
        },
        {
          "trait_type": "Nose",
          "value": "7"
        },
        {
          "trait_type": "Eyes",
          "value": "0.1"
        }
      ],
      "collection": {
        "name": "Demo1",
        "family": "Demo"
      },
      "properties": {
        "files": [
          {
            "uri": "https:/IMAGE_BASE_URL/6.png",
            "type": "image/png"
          }
        ],
        "category": "image",
        "creators": [
          {
            "address": "SC_WALLET",
            "share": 0.3,
            "verified": 1
          },
          {
            "address": "TAM_ADDRESS",
            "share": 0.7,
            "verified": 1
          }
        ],
        "collection_details": {
          "drop_price": 1,
          "primary_sales_royalty": 2,
          "secondary_market_royalty": 10,
          "drop_date": "Sat Jan 15 2022 03:00:00 GMT-0500 (Eastern Standard Time)"
        },
        "primary_contact": {
          "artist_username": "Tam",
          "artist_wallet_address": "TAM_ADDRESS",
          "artist_email": "tam@ay.com",
          "artist_phone_number ": "123456"
        }
      }
    },
    {
      "name": "SC Membership #7",
      "symbol": "SNOW",
      "description": "Test Description",
      "seller_fee_basis_points": 1000,
      "image": "https:/IMAGE_BASE_URL/7.png",
      "external_url": "https://snowcrash.com",
      "tokenId": 7,
      "attributes": [
        {
          "trait_type": "Face",
          "value": "Sea Green"
        },
        {
          "trait_type": "Nose",
          "value": "8"
        },
        {
          "trait_type": "Eyes",
          "value": "0.1"
        }
      ],
      "collection": {
        "name": "Demo1",
        "family": "Demo"
      },
      "properties": {
        "files": [
          {
            "uri": "https:/IMAGE_BASE_URL/7.png",
            "type": "image/png"
          }
        ],
        "category": "image",
        "creators": [
          {
            "address": "SC_WALLET",
            "share": 0.3,
            "verified": 1
          },
          {
            "address": "TAM_ADDRESS",
            "share": 0.7,
            "verified": 1
          }
        ],
        "collection_details": {
          "drop_price": 1,
          "primary_sales_royalty": 2,
          "secondary_market_royalty": 10,
          "drop_date": "Sat Jan 15 2022 03:00:00 GMT-0500 (Eastern Standard Time)"
        },
        "primary_contact": {
          "artist_username": "Tam",
          "artist_wallet_address": "TAM_ADDRESS",
          "artist_email": "tam@ay.com",
          "artist_phone_number ": "123456"
        }
      }
    },
    {
      "name": "SC Membership #8",
      "symbol": "SNOW",
      "description": "Test Description",
      "seller_fee_basis_points": 1000,
      "image": "https:/IMAGE_BASE_URL/8.png",
      "external_url": "https://snowcrash.com",
      "tokenId": 8,
      "attributes": [
        {
          "trait_type": "Face",
          "value": "Maroon"
        },
        {
          "trait_type": "Nose",
          "value": "9"
        },
        {
          "trait_type": "Eyes",
          "value": "0.1"
        }
      ],
      "collection": {
        "name": "Demo1",
        "family": "Demo"
      },
      "properties": {
        "files": [
          {
            "uri": "https:/IMAGE_BASE_URL/8.png",
            "type": "image/png"
          }
        ],
        "category": "image",
        "creators": [
          {
            "address": "SC_WALLET",
            "share": 0.3,
            "verified": 1
          },
          {
            "address": "TAM_ADDRESS",
            "share": 0.7,
            "verified": 1
          }
        ],
        "collection_details": {
          "drop_price": 1,
          "primary_sales_royalty": 2,
          "secondary_market_royalty": 10,
          "drop_date": "Sat Jan 15 2022 03:00:00 GMT-0500 (Eastern Standard Time)"
        },
        "primary_contact": {
          "artist_username": "Tam",
          "artist_wallet_address": "TAM_ADDRESS",
          "artist_email": "tam@ay.com",
          "artist_phone_number ": "123456"
        }
      }
    },
    {
      "name": "SC Membership #9",
      "symbol": "SNOW",
      "description": "Test Description",
      "seller_fee_basis_points": 1000,
      "image": "https:/IMAGE_BASE_URL/9.png",
      "external_url": "https://snowcrash.com",
      "tokenId": 9,
      "attributes": [
        {
          "trait_type": "Face",
          "value": "Sparkling Blue"
        },
        {
          "trait_type": "Nose",
          "value": "10"
        },
        {
          "trait_type": "Eyes",
          "value": "0.1"
        }
      ],
      "collection": {
        "name": "Demo1",
        "family": "Demo"
      },
      "properties": {
        "files": [
          {
            "uri": "https:/IMAGE_BASE_URL/9.png",
            "type": "image/png"
          }
        ],
        "category": "image",
        "creators": [
          {
            "address": "SC_WALLET",
            "share": 0.3,
            "verified": 1
          },
          {
            "address": "TAM_ADDRESS",
            "share": 0.7,
            "verified": 1
          }
        ],
        "collection_details": {
          "drop_price": 1,
          "primary_sales_royalty": 2,
          "secondary_market_royalty": 10,
          "drop_date": "Sat Jan 15 2022 03:00:00 GMT-0500 (Eastern Standard Time)"
        },
        "primary_contact": {
          "artist_username": "Tam",
          "artist_wallet_address": "TAM_ADDRESS",
          "artist_email": "tam@ay.com",
          "artist_phone_number ": "123456"
        }
      }
    }
  ]
  `
  function configure_minio(){
    try {
          var minioClient = new Minio.Client({
            endPoint: process.env.MINIO_ENDPOINT,
            // port: 9001,
            useSSL: true,
            accessKey: process.env.MINIO_ACCESS_KEY,
            secretKey: process.env.MINIo_SECRET_KEY,
        });
      return minioClient;

    } catch (error) {
      console.log(error)
    }

}
  function readJson() {

    var metaData = {
        'collection':{
          'family':'',
          'name':''
        }, 
        'attributes':[],
        'properties':{
          'files':[],
          'category':'',
          'creators':[],
          "collection_details": {
            "drop_price": 0,
            "primary_sales_royalty": 2,
            "secondary_market_royalty": 10,
            "drop_date": "Sat Jan 15 2022 03:00:00 GMT-0500 (Eastern Standard Time)"
          },
          "primary_contact": {
            "artist_username": "",
            "artist_wallet_address": "",
            "artist_email": "",
            "artist_phone_number ": ""
          },
        },
        'name':'',
        'description':'',
        'image':'',
        'external_url':'',
        'symbol':'',
        'seller_fee_basis_points':0
    }
    var parsedJson = JSON.parse(json)
    
    var tempmetaData = metaData;
    for(var i=0; i<1; i++){
      var obj = parsedJson[i]
      tempmetaData.collection.family = obj.collection.family
      tempmetaData.collection.name = obj.collection.name
      tempmetaData.name = obj.name
      tempmetaData.description = obj.description
      tempmetaData.image = obj.image
      tempmetaData.external_url = obj.external_url
      tempmetaData.symbol = obj.symbol
      tempmetaData.seller_fee_basis_points = obj.seller_fee_basis_points
      tempmetaData.attributes = obj.attributes
      // metaData.properties.files = obj.properties.files
      // metaData.properties.category = obj.properties.category
      // metaData.properties.creators = obj.properties.creators
      // metaData.properties.collection_details = obj.properties.collection_details
      // metaData.properties.primary_contact = obj.properties.primary_contact
    }
    execute_collections(tempmetaData)

    parsedJson.forEach(function (element) {
      // metaData.collection.family = element.collection.family
      // metaData.collection.name = element.collection.name
      // metaData.name = element.name
      // metaData.symbol = element.symbol
      // metaData.description = element.description
      // metaData.seller_fee_basis_points = element.seller_fee_basis_points
      // metaData.image = element.image
      // metaData.external_url = element.external_url

      var attributes = []
      element.attributes.forEach(function (attribute) {
        attributes.push({
          'trait_type':attribute.trait_type,
          'value':attribute.value
        })
      });
      
      element.properties.creators.forEach(function (creator) {
        metaData.properties.creators.push({
          'address':creator.address,
          'share':creator.share,
          'verified':creator.verified
        })
      });

      element.properties.files.forEach(function (file) {
        metaData.properties.files.push({
          'uri':file.uri,
          'type':file.type
        })
      });
      metaData.properties.category = element.properties.category;
      
      
      //Collection details
      metaData.properties.collection_details.drop_price = element.properties.collection_details.drop_price;
      metaData.properties.collection_details.primary_sales_royalty = element.properties.collection_details.primary_sales_royalty;
      metaData.properties.collection_details.secondary_market_royalty = element.properties.collection_details.secondary_market_royalty;
      metaData.properties.collection_details.drop_date = element.properties.collection_details.drop_date;
      
      //Primary details
      metaData.properties.primary_contact.artist_username = element.properties.primary_contact.artist_username;
      metaData.properties.primary_contact.artist_wallet_address = element.properties.primary_contact.artist_wallet_address;
      metaData.properties.primary_contact.artist_email = element.properties.primary_contact.artist_email;
      metaData.properties.primary_contact.artist_phone_number = element.properties.primary_contact.artist_phone_number;

      
      metaData.attributes = attributes
      db_query(metaData);
    })
    create_metadata(metaData)
  }

  
  function create_metadata (metaData){
    const artifact_id = cuid();
    const file_ext = process.env.FILE_EXTENSION
    const reference_id = cuid();
    const slug_id = slug.nice();
    const storage_location = process.env.STORAGE_LOCATION;
    const public_storage_location = process.env.PUBLIC_STORAGE_LOCATION;
    const host_name = process.env.HOSTNAME;
    const upload_path = `${reference_id}/${artifact_id}.${file_ext}`;
    const public_path = `${host_name}/${public_storage_location}/${slug_id}.${file_ext}`;
    if ( metaData.properties.collection_details && metaData.properties.primary_contact){
      delete metaData.properties.collection_details;
      delete metaData.properties.primary_contact
         
      try {
        if (minioClient.bucketExists(storage_location)) {
          var buffer = Buffer.from(JSON.stringify (metaData)); // Read file from pre-drop bucket
          minioClient.putObject(storage_location, upload_path, buffer, function(err, etag) {
            return console.log(err, etag) // err should be null
          })  
          const minioClient = configure_minio()
        }

      } catch (err) {
        console.log(`Something went wrong while connecting to MinIO: ${err.message}`)
      }
      
    }
    
  }

function db_config() {
  try {
    const pool = new Pool({
        user: process.env.DB_USER,
        host: process.env.DB_HOST,
        database: process.env.DB_NAME,
        password: process.env.DB_PASSWORD,
        port: process.env.DB_PORT,
      });
    return pool;
  } catch (error) {
    console.log(error.message)
  }
}

var pool
console.log('generating uuid')
var uuid = uuidv4.v4();
var status = 'RTP'

if ( pool === undefined ){
  console.log('pool called!')
  pool= db_config()
}

// Executing collections only
function execute_collections ( tempmetaData ){
  console.log('Executing collection query')
  console.log(`UUID: ${uuid}`)
  console.log(`NAME: ${tempmetaData.collection.name}`)
  console.log(`FAMILY: ${tempmetaData.collection.family}`)
  console.log(true)
  console.log(`STATUS: ${status}`)
  try {
    pool.query('INSERT INTO collections (ID, name, family, active, status) VALUES ($1, $2, $3, $4, $5)', 
    [
      uuid, tempmetaData.collection.name, tempmetaData.collection.family, true, status
    ], (err, res) => {
      console.log(res)
      if ( err){
        console.error(err.message)
      }
    });  
  } catch (error) {
    console.log(error.message)
  }
  
}


// Executiion of queries
function db_query( data ) {
  console.log('Executing nft query')
  pool.query(`INSERT INTO nft(id,collections_id, status) VALUES($1, $2, $3)`, [uuid, uuid, status], (err, res) => {
    console.log(res)
    if ( err){
      console.error(err.message)
    }

  });
  
  console.log('Executing nft_metadata query')
  pool.query(`INSERT INTO nft_metadata(id, name, symbol, description, seller_fee_basis_points, image_url, status) VALUES($1, $2, $3, $4, $5, $6, $7)`, 
  [uuid, data.name, data.symbol, data.description, data.seller_fee_basis_points, data.image, status], (err, res) => {
    console.log(res)
    if ( err){
      console.error(err.message)
    }
  }) 
  
  console.log('Executing attributes query')
  data.attributes.forEach(function (attribute) {
    
    pool.query(`INSERT INTO attributes(nft_metadata_id) VALUES($1)`, 
      [uuid], (err, res) => {
      console.log(res)
      if ( err ){
        console.error(err.message)
      }
    }) 
  });
  
  let collection_details = {
    drop_date:'',
    primary_sales_royalty: '',
    secondary_market_royalty: '',
    drop_price: 0,
  }
  collection_details.drop_date = new Date(data.properties.collection_details.drop_date)
  collection_details.primary_sales_royalty = data.properties.collection_details.primary_sales_royalty
  collection_details.secondary_market_royalty = data.properties.collection_details.secondary_market_royalty
  collection_details.drop_price = data.properties.collection_details.drop_price
  
  console.log(`Collection_details`, collection_details);
  
  console.log('Executing properties query');
  pool.query(`INSERT INTO properties(id, nft_metadata_id) VALUES($1, $2)`, 
    [uuid, uuid], (err, res) => {
    console.log(res)
    if ( err ){
      console.error(err.message)
    }
  })

  console.log('Executing creators query');
  data.properties.creators.forEach(function (creator) { 
    pool.query(`INSERT INTO creators(id, address, share, verified, nft_metadata_id) VALUES($1, $2, $3, $4, $5)`, 
      [uuidv4.v4(), creator.address, creator.share, creator.verified, uuid], (err, res) => {
      console.log(res)
      if ( err ){
        console.error(err.message)
      }
    })
  })

  let metaData = {
    properties: {
      primary_contact:{
        artist_username: '',
        artist_wallet_address: '',
        artist_email: '',
        artist_phone_number: '',
      }
    }
  }  
  metaData.properties.primary_contact.artist_username = data.properties.primary_contact.artist_username;
  metaData.properties.primary_contact.artist_wallet_address = data.properties.primary_contact.artist_wallet_address;
  metaData.properties.primary_contact.artist_email = data.properties.primary_contact.artist_email;
  metaData.properties.primary_contact.artist_phone_number = data.properties.primary_contact.artist_phone_number;
  
  console.log(`Primary Contact`, metaData.properties.primary_contact);

  console.log('Executing wallets query');
  pool.query(`INSERT INTO wallets(id, user_id, status, active) VALUES($1, $2, $3, $4)`, [uuidv4.v4(), uuid, status, true], (err, res) => {
    console.log(res)
    if ( err ){
      console.error(err.message)
    }
  });

}
readJson();