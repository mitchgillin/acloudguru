import {
  DynamoDBClient,
  ListTablesCommand,
  CreateTableCommand,
  BatchWriteItemCommand,
} from "@aws-sdk/client-dynamodb";

import fs from "fs";
import { parse } from "csv-parse";

const listTables = async (client) => {
  const command = new ListTablesCommand({});

  try {
    const results = await client.send(command);
    return results;
  } catch (e) {
    console.error(e);
  }
};

const createTable = async (client) => {
  const params = {
    AttributeDefinitions: [
      {
        AttributeName: "id",
        AttributeType: "N",
      },
      {
        AttributeName: "name",
        AttributeType: "S",
      },
    ],
    KeySchema: [
      {
        AttributeName: "id",
        KeyType: "HASH",
      },
      {
        AttributeName: "name",
        KeyType: "RANGE",
      },
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: 1,
      WriteCapacityUnits: 1,
    },
    TableName: "pinehead",
  };

  const command = new CreateTableCommand(params);
  const response = await client.send(command);

  return response;
};

const exponentialBackoff = async (batch, attempt = 0) => {
  const client = new DynamoDBClient({ region: "us-east-1" });
  const delay = (duration) =>
    new Promise((resolve) => setTimeout(resolve, duration));

  const params = {
    RequestItems: {
      pinehead: batch,
    },
  };

  try {
    const command = new BatchWriteItemCommand(params);
    await client.send(command);
    console.log(`Batch successfully inserted after ${attempt} attempt(s)`);
  } catch (error) {
    if (attempt < 5) {
      // Retry up to 5 times
      console.log(`Attempt ${attempt + 1}: Retrying batch insert...`);
      await delay(50 * Math.pow(2, attempt)); // Exponential backoff formula
      await exponentialBackoff(batch, attempt + 1);
    } else {
      console.error(
        "Max retry attempts reached. Failed to insert batch:",
        error
      );
    }
  }
};

const main = async () => {
  const client = new DynamoDBClient({ region: "us-east-1" });
  // See if there are any tables
  const tables = await listTables(client);
  if (!tables.TableNames.includes("pinehead")) {
    console.log(tables, "Create new Table");
    const tableCreationResponse = await createTable(client);
    console.log({ tableCreationResponse });
  } else {
    let data = [];
    console.log(tables, "Table Exists!");
    fs.createReadStream("./artist.csv").pipe(
      parse({ delimiter: "Ԙ", quote: "ԡ", from_line: 2 })
        .on("data", (row) => {
          data = [
            ...data,
            {
              PutRequest: { Item: { id: { N: row[0] }, name: { S: row[1] } } },
            },
          ];
        })
        .on("end", async () => {
          for (let i = 0; i < data.length; i += 25) {
            const batch = data.slice(i, i + 25);
            exponentialBackoff(batch, 0);
          }
        })
    );
  }
};

main();
