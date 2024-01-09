import {
  AttributeValue,
  DeleteItemCommandInput,
  DynamoDB,
  GetItemCommandInput,
  PutItemCommandInput,
  QueryCommandInput,
  ScanCommandInput,
  ScanCommandOutput,
} from '@aws-sdk/client-dynamodb';

export default class DynamoRepository {
  protected dynamoDB: DynamoDB;

  constructor(protected readonly region: string) {
    try {
      this.dynamoDB = new DynamoDB({
        region,
      });
    } catch (e) {
      console.error(e.message ?? e, e?.stack, e?.name);
    }
  }

  async findAllTables(): Promise<any> {
    try {
      return await this.dynamoDB.listTables({});
    } catch (error) {
      console.error(error);
    }
  }

  async findOneByKey(
    params: GetItemCommandInput,
  ): Promise<Record<string, AttributeValue>> {
    try {
      return (await this.dynamoDB.getItem(params)).Item;
    } catch (error) {
      console.error(error);
    }
  }

  async save(params: PutItemCommandInput) {
    try {
      return this.dynamoDB.putItem(params);
    } catch (error) {
      console.error('SaveDynamoData', error);
      throw new Error(error);
    }
  }

  async delete(params: DeleteItemCommandInput): Promise<any> {
    try {
      return this.dynamoDB.deleteItem(params);
    } catch (error) {
      console.error;
    }
  }

  async scan(params: ScanCommandInput): Promise<any> {
    try {
      return (await this.dynamoDB.scan(params)).Items;
    } catch (error) {
      console.error(error);
    }
  }

  async query(params: QueryCommandInput): Promise<any> {
    return new Promise((resolve, reject) => {
      this.dynamoDB.query(params, (err, data) => {
        if (err) reject(err);
        else resolve(data);
      });
    });
  }

  async parallelScanWithPagination(
    params: ScanCommandInput,
    numSegments: number,
    pageSize: number,
    pageNumber: number,
  ): Promise<ScanCommandOutput> {
    const segments: ScanCommandInput[] = [];
    for (let i = 0; i < numSegments; i++) {
      segments.push({
        ...params,
        Segment: i,
        TotalSegments: numSegments,
      });
    }

    const scanPromises: Promise<ScanCommandOutput>[] = segments.map((segment) =>
      this.dynamoDB.scan(segment),
    );
    const scanResults: ScanCommandOutput[] = await Promise.all(scanPromises);

    let lastEvaluatedKey: { [key: string]: any } | undefined;
    const mergedResult: ScanCommandOutput = {
      Items: [],
      Count: 0,
      ScannedCount: 0,
      $metadata: undefined,
    };

    for (const result of scanResults) {
      mergedResult.Items.push(...(result.Items || []));
      mergedResult.Count += result.Count || 0;
      mergedResult.ScannedCount += result.ScannedCount || 0;
      if (result.LastEvaluatedKey) {
        lastEvaluatedKey = result.LastEvaluatedKey;
      }
    }

    if (lastEvaluatedKey) {
      mergedResult.LastEvaluatedKey = lastEvaluatedKey;
    }

    const paginatedResult = mergedResult.Items.slice(
      (pageNumber - 1) * pageSize,
      pageNumber * pageSize,
    );
    mergedResult.Items = paginatedResult;

    return mergedResult;
  }

  async parallelScan(
    params: ScanCommandInput,
    numSegments: number,
  ): Promise<ScanCommandOutput> {
    const segments: ScanCommandInput[] = [];
    for (let i = 0; i < numSegments; i++) {
      segments.push({
        ...params,
        Segment: i,
        TotalSegments: numSegments,
      });
    }

    const scanPromises: Promise<ScanCommandOutput>[] = segments.map((segment) =>
      this.dynamoDB.scan(segment),
    );
    const scanResults: ScanCommandOutput[] = await Promise.all(scanPromises);

    let lastEvaluatedKey: { [key: string]: any } | undefined;
    const mergedResult: ScanCommandOutput = {
      Items: [],
      Count: 0,
      ScannedCount: 0,
      $metadata: undefined,
    };

    for (const result of scanResults) {
      mergedResult.Items.push(...(result.Items || []));
      mergedResult.Count += result.Count || 0;
      mergedResult.ScannedCount += result.ScannedCount || 0;
      if (result.LastEvaluatedKey) {
        lastEvaluatedKey = result.LastEvaluatedKey;
      }
    }

    if (lastEvaluatedKey) {
      mergedResult.LastEvaluatedKey = lastEvaluatedKey;
    }

    return mergedResult;
  }
}
