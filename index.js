const AWS = require('aws-sdk');
const crypto = require('crypto');


const dynamo = new AWS.DynamoDB.DocumentClient();
const S3 = new AWS.S3();
const cognito = new AWS.CognitoIdentityServiceProvider();


const region = 'eu-west-1';
const tableName = 'coolbooks-marketplace';


exports.handler = async (event, context) => {
    let body;
    let statusCode = '200';
    const headers = {
        'Content-Type': 'application/json',
    };
   
    const userId = event.requestContext.authorizer.claims['cognito:username'];


    try {
        switch (event.httpMethod) {
            case 'GET':
                const types = await getUserTypes(userId);
                body = await getBooksByTypesExcludeUser(types, userId);
                break;
            case 'POST':
                let data = JSON.parse(event.body);
                data.id = AWS.util.uuid.v4();
                data.user_id = userId;
                data.date = Math.floor(new Date().getTime() / 1000);
                if(data.picture) data.picture = await uploadOnS3(data.picture);
                body = await dynamo.put({TableName: tableName,Item: data}).promise();
                statusCode = 201;
                break;
            default:
                throw new Error(`Unsupported method "${event.httpMethod}"`);
        }
    } catch (err) {
        statusCode = '500';
        body = 'Internal Server Error';//err.message;
    } finally {
        body = JSON.stringify(body);
    }


    return {
        statusCode,
        body,
        headers,
    };
};


async function getUserTypes(userId){
    return (await dynamo.query({
        TableName: tableName,
        IndexName: 'user_id-index',
        KeyConditionExpression: '#user_id = :user_id',
        ExpressionAttributeValues: {
            ':user_id':  userId,
        },
        ExpressionAttributeNames: {
            '#user_id': 'user_id',
        },
     }).promise())
        .Items
        .map(v => v.desire_type)
        .reduce((a, v) => a.concat(v), [])
        .reduce((a, v) => a.includes(v) ? a : [...a, v], []);
}


async function getBooksByTypesExcludeUser(types, userIdToBeExcluded){
    const userPoolId = 'eu-west-1_pQF3vlfni';
    let booksPromises = []
    for(let type of types){
        booksPromises.push(
            dynamo.query({
                TableName: tableName,
                IndexName: 'type-index',
                KeyConditionExpression: '#type = :type',
                FilterExpression: '#user_id <> :user_id',
                ExpressionAttributeValues: {
                    ':type': type,
                    ':user_id': userIdToBeExcluded,
                },
                ExpressionAttributeNames: {
                    '#type': 'type',
                    '#user_id': 'user_id',
                },
             }).promise()
        )
    }
    let books = await Promise.all(booksPromises);
    books = books.map(v => v.Items);
    books = [].concat(...books);
    const usersIds = new Set(books.map(v => v.user_id));
    let usersPromises = []
    for(let userId  of usersIds){
        usersPromises.push(await cognito.adminGetUser({
            UserPoolId: userPoolId,
            Username: userId,
        }).promise())
    }
    const users = await Promise.all(usersPromises);
    let usersMap = {}
    for(let user of users) {
        usersMap[user.Username] = user
            .UserAttributes
            .filter(v => v.Name == 'name')
            .reduce((a, v) => {a[v.Name] = v.Value; return a;}, {});
    }
    for(let book of books) {
        book.user = usersMap[book.user_id];
    }
    return books;
}


async function uploadOnS3(picture){
    const bucketName = 'coolbooks';
    const matches = picture.match(/^data:(.+);base64,(.+)$/);
    const imgName = 'pictures/'+crypto.randomBytes(16).toString('hex')+'.'+(matches[1].split('/'))[1];
    await S3.putObject({
        Bucket: bucketName,
        Key: imgName,
        Body: Buffer.from(matches[2], 'base64'),
        ContentType: matches[1],
        ACL: 'public-read'
    }).promise(); 
    return `https://${bucketName}.s3.${region}.amazonaws.com/${imgName}`;
}
